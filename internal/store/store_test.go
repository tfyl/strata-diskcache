package store

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fp = "0123456789ab"

func key(i int) string { return fmt.Sprintf("%064x", i) }

func name(i, tokens int) string { return FileName(fp, key(i), tokens) }

func write(t *testing.T, dir, name string, size int, mtime time.Time) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func exists(dir, name string) bool { _, err := os.Stat(filepath.Join(dir, name)); return err == nil }

func newStore(t *testing.T, maxBytes, free int64) (*Store, string, map[string]int) {
	t.Helper()
	dir := t.TempDir()
	evicted := map[string]int{}
	s := New(Options{Dir: dir, MaxBytes: maxBytes, MaxAge: 7 * 24 * time.Hour, MinFree: 100,
		OnEvict:   func(why string, _ int64) { evicted[why]++ },
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		FreeSpace: func(string) (int64, error) { return free, nil }})
	return s, dir, evicted
}

func TestLoadCleansUp(t *testing.T) {
	s, dir, _ := newStore(t, 1000, 1<<40)
	now := time.Now()
	write(t, dir, name(1, 20000), 10, now.Add(-8*24*time.Hour)) // past the age limit
	write(t, dir, name(2, 20000), 10, now.Add(-time.Hour))
	write(t, dir, name(3, 20000), 0, now)                        // empty: a broken write
	write(t, dir, "dc-"+key(4)+"-20000.session", 10, now)        // an older format
	write(t, dir, ".dc-abc.1234.tmp", 10, now.Add(-2*time.Hour)) // a dead save's temporary
	write(t, dir, ".dc-def.5678.tmp", 10, now)                   // maybe a save in progress
	write(t, dir, "saved-by-hand.bin", 10, now.Add(-30*24*time.Hour))
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]bool{
		name(1, 20000): false, name(2, 20000): true, name(3, 20000): false,
		"dc-" + key(4) + "-20000.session": false, ".dc-abc.1234.tmp": false,
		".dc-def.5678.tmp": true, "saved-by-hand.bin": true,
	} {
		if exists(dir, file) != want {
			t.Errorf("%s: exists=%v, want %v", file, !want, want)
		}
	}
	if n, _ := s.Stats(); n != 1 {
		t.Errorf("%d files indexed, want 1", n)
	}
}

func TestFingerprint(t *testing.T) {
	s, dir, evicted := newStore(t, 1000, 1<<40)
	write(t, dir, name(1, 20000), 10, time.Now())
	write(t, dir, FileName("ffffffffffff", key(2), 20000), 10, time.Now())
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	s.KeepFingerprint(fp)
	if !exists(dir, name(1, 20000)) || exists(dir, FileName("ffffffffffff", key(2), 20000)) || evicted["fingerprint"] != 1 {
		t.Errorf("KeepFingerprint kept the wrong files: %v", evicted)
	}
}

func TestCapAndLRU(t *testing.T) {
	s, dir, _ := newStore(t, 250, 1<<40)
	now := time.Now()
	for i := 1; i <= 4; i++ { // 1 is the least recently used
		write(t, dir, name(i, 20000), 100, now.Add(time.Duration(i-10)*time.Minute))
	}
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if !exists(dir, name(3, 20000)) || !exists(dir, name(4, 20000)) || exists(dir, name(2, 20000)) {
		t.Fatal("the cap kept the wrong files")
	}
	s.Touch(Entry{Fingerprint: fp, Key: key(3), Tokens: 20000}) // 3 is now the most recent
	if !s.Reserve(100) || exists(dir, name(4, 20000)) || !exists(dir, name(3, 20000)) {
		t.Fatal("Reserve evicted the wrong file")
	}
	if s.Reserve(300) {
		t.Error("a file bigger than the cap fits")
	}
}

func TestDiskFloorWins(t *testing.T) {
	s, dir, evicted := newStore(t, 1<<30, 0)
	s.opt.MinFree = 1000
	for i := 1; i <= 3; i++ {
		write(t, dir, name(i, 20000), 100, time.Now().Add(time.Duration(i)*time.Second))
	}
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	s.opt.FreeSpace = func(string) (int64, error) { return 1050, nil } // needs 50 more: the oldest goes
	if !s.Reserve(100) || exists(dir, name(1, 20000)) || !exists(dir, name(2, 20000)) {
		t.Fatal("wrong eviction for the disk floor")
	}
	s.opt.FreeSpace = func(string) (int64, error) { return 500, nil } // no room even with every file gone
	if s.Reserve(100) {
		t.Fatal("saved below the disk floor")
	}
	if n, _ := s.Stats(); n != 2 || evicted["disk"] != 1 {
		t.Errorf("a hopeless reserve evicted files: %d left, %v", n, evicted)
	}
	s.opt.FreeSpace = func(string) (int64, error) { return 0, os.ErrPermission }
	if s.Reserve(1) {
		t.Error("statfs failing still reserved")
	}
}

func TestAnyFingerprint(t *testing.T) {
	s, dir, _ := newStore(t, 1<<30, 1<<40)
	if s.AnyFingerprint() != "" {
		t.Error("an empty store gave a fingerprint")
	}
	now := time.Now()
	write(t, dir, FileName("aaaaaaaaaaaa", key(1), 20000), 10, now.Add(-2*time.Hour))
	write(t, dir, FileName(fp, key(2), 20000), 10, now.Add(-time.Hour))
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if got := s.AnyFingerprint(); got != fp {
		t.Errorf("AnyFingerprint = %q, want the most recently used file's", got)
	}
	s.Touch(Entry{Fingerprint: "aaaaaaaaaaaa", Key: key(1), Tokens: 20000})
	if got := s.AnyFingerprint(); got != "aaaaaaaaaaaa" {
		t.Errorf("After a Touch, AnyFingerprint = %q, want the file just used", got)
	}
}

func TestAnyFingerprintTie(t *testing.T) {
	s, dir, _ := newStore(t, 1<<30, 1<<40)
	now := time.Now()
	write(t, dir, FileName("aaaaaaaaaaaa", key(1), 20000), 10, now)
	write(t, dir, FileName("bbbbbbbbbbbb", key(2), 20000), 10, now)
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ { // map order is randomised: the tie must break the same way
		if got := s.AnyFingerprint(); got != "bbbbbbbbbbbb" {
			t.Fatalf("AnyFingerprint = %q, want the higher file name for the same mtime", got)
		}
	}
}

func TestCommitLookupAndCorruptFiles(t *testing.T) {
	s, dir, _ := newStore(t, 1<<30, 1<<40)
	t1 := []string{key(1)}
	t2 := []string{key(1), key(2), key(3)}
	t3 := []string{key(1), key(2), key(3), key(4), key(5)}

	write(t, dir, name(1, 20000), 10, time.Now())
	if _, err := s.Commit(fp, t1, 20000, 20000); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.Lookup(fp, t3); !ok || e.Key != key(1) {
		t.Fatal("turn 3 does not find turn 1's file")
	}
	if _, ok := s.Lookup("ffffffffffff", t3); ok {
		t.Error("found under another fingerprint")
	}
	if !s.Covered(fp, t2, 20000) || s.Covered(fp, t2, 20001) {
		t.Error("Covered is wrong")
	}

	// Strata counted 30123 tokens where 30000 were planned: renamed, and turn 1's file superseded
	write(t, dir, name(3, 30000), 10, time.Now())
	if e, err := s.Commit(fp, t2, 30000, 30123); err != nil || e.Tokens != 30123 || !exists(dir, name(3, 30123)) {
		t.Fatalf("commit with another count: %+v %v", e, err)
	}
	if exists(dir, name(1, 20000)) {
		t.Error("turn 1's file not superseded")
	}
	if _, err := s.Commit(fp, t3, 1, 1); err == nil {
		t.Error("committed a file Strata did not write")
	}

	// truncated behind our back: dropped on lookup
	if err := os.Truncate(filepath.Join(dir, name(3, 30123)), 5); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Lookup(fp, t3); ok || exists(dir, name(3, 30123)) {
		t.Error("a changed file was restored")
	}
}
