package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tfyl/strata-diskcache/internal/strata/stratatest"
)

func TestConfig(t *testing.T) {
	c, err := parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8080" || c.Upstream != "http://127.0.0.1:8081" || c.MaxGiB != 40 || c.RetentionDays != 7 ||
		c.ParkMinTokens != -1 || c.Log.Output != "stderr" || !c.Log.Compress {
		t.Errorf("defaults: %+v", c)
	}

	t.Setenv("SESSION_DIR", "/var/lib/sessions")
	t.Setenv("MAX_GIB", "10")
	t.Setenv("LOG_COMPRESS", "false")
	c, err = parse([]string{"--max-gib", "20"})
	if err != nil || c.SessionDir != "/var/lib/sessions" || c.MaxGiB != 20 || c.Log.Compress {
		t.Errorf("env, then the flag: %+v %v", c, err)
	}

	for _, args := range [][]string{{"--retention-days", "0"}, {"--upstream", "127.0.0.1:8081"}, {"--log-output", "syslog"}, {"--nope"}} {
		if _, err := parse(args); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

func TestLogFileRotates(t *testing.T) {
	dir := t.TempDir()
	c, _ := parse([]string{"--log-output", "both", "--log-file", filepath.Join(dir, "logs", "p.log"),
		"--log-format", "json", "--log-max-size-mib", "1", "--log-max-backups", "2"})
	var stderr bytes.Buffer
	log, closer, err := newLogger(c, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("x", 1000)
	for i := 0; i < 3500; i++ { // ~3.5 MiB: three rotations
		log.Info("request", "pad", pad)
	}
	_ = closer.Close()
	if !strings.Contains(stderr.String(), `"msg":"request"`) {
		t.Error("nothing on stderr with --log-output both")
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) { // gzip runs in the background
		gz, _ := filepath.Glob(filepath.Join(dir, "logs", "p-*.log.gz"))
		plain, _ := filepath.Glob(filepath.Join(dir, "logs", "p-*.log"))
		if len(gz) == 2 && len(plain) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("want 2 gzipped backups and none plain, got %v %v", gz, plain)
		}
	}
	before, _ := os.Stat(filepath.Join(dir, "logs", "p.log"))
	log2, closer2, _ := newLogger(c, io.Discard) // a restart appends
	log2.Info("after restart")
	_ = closer2.Close()
	if after, _ := os.Stat(filepath.Join(dir, "logs", "p.log")); after.Size() <= before.Size() {
		t.Error("a restart truncated the log file")
	}
}

func TestShutdownMidSave(t *testing.T) {
	dir := t.TempDir()
	f := stratatest.New(dir)
	up := httptest.NewServer(f)
	defer up.Close()
	c, err := parse([]string{"--upstream", up.URL, "--session-dir", dir, "--idle-save-s", "0.02", "--shutdown-s", "5"})
	if err != nil {
		t.Fatal(err)
	}
	pl, _ := net.Listen("tcp", "127.0.0.1:0")
	al, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx, c, slog.New(slog.NewTextHandler(io.Discard, nil)), pl, al) }()

	f.Set(20000, 0)
	entered := f.Entered()
	f.Update(func(f *stratatest.Fake) { f.SlotDelay = 400 * time.Millisecond })
	resp, err := http.Post("http://"+pl.Addr().String()+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if got := <-entered; got != "save" {
		t.Fatalf("slot call %q", got)
	}
	cancel() // SIGTERM while the save runs
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung")
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "dc-*.session")); len(m) != 1 {
		t.Errorf("the save did not finish before exit: %v", m)
	}
	if r, err := http.Get("http://" + al.Addr().String() + "/health"); err == nil {
		_ = r.Body.Close()
		t.Error("the admin server is still up")
	}
}
