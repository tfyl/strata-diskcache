// Package store indexes the session files the proxy owns and enforces their
// limits: an age limit, a total size cap and a free-space floor.
//
// Files are named dc-<fingerprint>-<key>-<tokens>.session in Strata's
// slot_save_path. The fingerprint names the Strata build, model and settings
// that wrote the file; Strata refuses a file of another one, so those go. A
// file's mtime is its last use (save or restore), for the LRU and the age
// limit. Other files in the folder are never touched. Strata writes a file
// under a temporary name and renames it into place, so a file under its final
// name is complete; a leftover temporary (.dc-*.tmp) is from a save that died.
package store

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Options are the store's folder and limits.
type Options struct {
	Dir      string
	MaxBytes int64
	MaxAge   time.Duration
	MinFree  int64
	OnEvict  func(reason string, bytes int64) // optional
	Log      *slog.Logger
	// for tests; default to statfs and the wall clock
	FreeSpace func(dir string) (int64, error)
	Now       func() time.Time
}

// Entry is one file.
type Entry struct {
	Fingerprint, Key string
	Tokens           int
	Size             int64
	Used             time.Time
}

// FileName is the name of the file for this fingerprint, key and token count.
func FileName(fingerprint, key string, tokens int) string {
	return fmt.Sprintf("dc-%s-%s-%d.session", fingerprint, key, tokens)
}

// Name is the entry's file name.
func (e *Entry) Name() string { return FileName(e.Fingerprint, e.Key, e.Tokens) }

var (
	nameRe = regexp.MustCompile(`^dc-([0-9a-f]{12})-([0-9a-f]{64})-([1-9][0-9]*)\.session$`)
	tempRe = regexp.MustCompile(`^\.dc-.*\.tmp$`)
)

// Store is the index. It is safe for concurrent use.
type Store struct {
	opt     Options
	mu      sync.Mutex
	entries map[string]*Entry // by fingerprint/key
}

// New returns an empty store; Load reads the folder.
func New(opt Options) *Store {
	if opt.FreeSpace == nil {
		opt.FreeSpace = freeSpace
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.OnEvict == nil {
		opt.OnEvict = func(string, int64) {}
	}
	if opt.Log == nil {
		opt.Log = slog.Default()
	}
	return &Store{opt: opt, entries: map[string]*Entry{}}
}

func freeSpace(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * st.Bsize, nil
}

// Load reads the folder: our files into the index; empty files, files in an
// unknown format and leftover temporaries removed; then the limits applied.
func (s *Store) Load() error {
	ents, err := os.ReadDir(s.opt.Dir)
	if err != nil {
		return err
	}
	s.mu.Lock()
	for _, d := range ents {
		name := d.Name()
		info, err := d.Info()
		switch {
		case err != nil || !info.Mode().IsRegular():
		case tempRe.MatchString(name):
			if s.opt.Now().Sub(info.ModTime()) > time.Hour { // a recent one may still be written
				s.remove(name, "stale_temporary", info.Size())
			}
		case !strings.HasPrefix(name, "dc-") || !strings.HasSuffix(name, ".session"):
			// not ours
		default:
			m := nameRe.FindStringSubmatch(name)
			if m == nil || info.Size() == 0 {
				s.remove(name, "unreadable", info.Size()) // an older format, or a broken write
				continue
			}
			tokens, _ := strconv.Atoi(m[3])
			e := &Entry{Fingerprint: m[1], Key: m[2], Tokens: tokens, Size: info.Size(), Used: info.ModTime()}
			if old := s.entries[m[1]+"/"+m[2]]; old != nil { // one key, two token counts: keep the longer
				if old.Tokens >= tokens {
					s.remove(name, "duplicate", e.Size)
					continue
				}
				s.drop(old, "duplicate")
			}
			s.entries[m[1]+"/"+m[2]] = e
		}
	}
	s.mu.Unlock()
	s.Prune()
	return nil
}

func (s *Store) remove(name, why string, size int64) {
	if err := os.Remove(filepath.Join(s.opt.Dir, name)); err != nil && !os.IsNotExist(err) {
		s.opt.Log.Warn("remove file", "file", name, "reason", why, "err", err)
		return
	}
	s.opt.Log.Info("evict", "file", name, "reason", why, "bytes", size)
	s.opt.OnEvict(why, size)
}

// drop removes e from the index and the disk; mu held.
func (s *Store) drop(e *Entry, why string) {
	delete(s.entries, e.Fingerprint+"/"+e.Key)
	s.remove(e.Name(), why, e.Size)
}

// byAge lists the entries, least recently used first; mu held.
func (s *Store) byAge() []*Entry {
	l := make([]*Entry, 0, len(s.entries))
	for _, e := range s.entries {
		l = append(l, e)
	}
	slices.SortFunc(l, func(a, b *Entry) int { return a.Used.Compare(b.Used) })
	return l
}

// Stats is the number of files and their total size.
func (s *Store) Stats() (files int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries), s.total()
}

func (s *Store) total() (t int64) {
	for _, e := range s.entries {
		t += e.Size
	}
	return t
}

// Prune applies the age limit and the size cap, oldest use first.
func (s *Store) Prune() {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.total()
	for _, e := range s.byAge() {
		if s.opt.Now().Sub(e.Used) > s.opt.MaxAge {
			total -= e.Size
			s.drop(e, "age")
		} else if total > s.opt.MaxBytes {
			total -= e.Size
			s.drop(e, "size")
		}
	}
}

// KeepFingerprint removes every file written under another fingerprint.
func (s *Store) KeepFingerprint(fingerprint string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.byAge() {
		if e.Fingerprint != fingerprint {
			s.drop(e, "fingerprint")
		}
	}
}

// AnyFingerprint returns the fingerprint of the most recently used session file on disk, or "".
func (s *Store) AnyFingerprint() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *Entry
	for _, e := range s.entries {
		if best == nil || e.Used.After(best.Used) || (e.Used.Equal(best.Used) && e.Name() > best.Name()) {
			best = e
		}
	}
	if best != nil {
		return best.Fingerprint
	}
	return ""
}

// Reserve makes room for a file of about need bytes: the least recently used
// files go until it fits under the cap and leaves MinFree on the disk. False
// when it cannot fit even with every file of ours gone; then nothing goes.
func (s *Store) Reserve(need int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	free, err := s.opt.FreeSpace(s.opt.Dir)
	if err != nil {
		s.opt.Log.Warn("free space", "dir", s.opt.Dir, "err", err)
		return false
	}
	total := s.total()
	if need > s.opt.MaxBytes || free+total-need < s.opt.MinFree {
		return false
	}
	for _, e := range s.byAge() {
		if total+need <= s.opt.MaxBytes && free-need >= s.opt.MinFree {
			break
		}
		why := "size"
		if total+need <= s.opt.MaxBytes {
			why = "disk"
		}
		total, free = total-e.Size, free+e.Size
		s.drop(e, why)
	}
	return true
}

// Lookup finds the file of the longest earlier turn of a conversation whose
// prefix keys are keys (shortest first). A file that is gone or changed size
// since it was indexed is dropped and the search goes on.
func (s *Store) Lookup(fingerprint string, keys []string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(keys) - 1; i >= 0; i-- {
		e := s.entries[fingerprint+"/"+keys[i]]
		if e == nil {
			continue
		}
		if info, err := os.Stat(filepath.Join(s.opt.Dir, e.Name())); err != nil || info.Size() != e.Size {
			s.drop(e, "changed")
			continue
		}
		return *e, true
	}
	return Entry{}, false
}

// Covered reports whether a file of one of keys holds at least tokens
// tokens. A file of an earlier turn still counts: the engine reads only what
// came after it.
func (s *Store) Covered(fingerprint string, keys []string, tokens int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		if e := s.entries[fingerprint+"/"+k]; e != nil && e.Tokens >= tokens {
			return true
		}
	}
	return false
}

// Commit indexes the file Strata just saved as FileName(fingerprint,
// keys[last], planned), renamed to carry Strata's own token count when that
// differs. The files of the conversation's earlier turns are superseded.
func (s *Store) Commit(fingerprint string, keys []string, planned, tokens int) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := keys[len(keys)-1]
	e := &Entry{Fingerprint: fingerprint, Key: key, Tokens: planned}
	if tokens > 0 && tokens != planned {
		from := filepath.Join(s.opt.Dir, e.Name())
		e.Tokens = tokens
		if err := os.Rename(from, filepath.Join(s.opt.Dir, e.Name())); err != nil {
			e.Tokens = planned
		}
	}
	info, err := os.Stat(filepath.Join(s.opt.Dir, e.Name()))
	if err != nil {
		return Entry{}, fmt.Errorf("saved file: %w", err)
	}
	e.Size, e.Used = info.Size(), info.ModTime()
	for _, k := range keys {
		if old := s.entries[fingerprint+"/"+k]; old != nil && old.Name() != e.Name() {
			s.drop(old, "superseded")
		}
	}
	s.entries[fingerprint+"/"+key] = e
	return *e, nil
}

// Touch marks e used now (a restore).
func (s *Store) Touch(e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur := s.entries[e.Fingerprint+"/"+e.Key]; cur != nil {
		cur.Used = s.opt.Now()
		_ = os.Chtimes(filepath.Join(s.opt.Dir, cur.Name()), cur.Used, cur.Used)
	}
}

// Forget drops e from the index and the disk.
func (s *Store) Forget(e Entry, why string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur := s.entries[e.Fingerprint+"/"+e.Key]; cur != nil && cur.Name() == e.Name() {
		s.drop(cur, why)
	}
}
