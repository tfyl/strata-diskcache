package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tfyl/strata-diskcache/internal/store"
	"github.com/tfyl/strata-diskcache/internal/strata/stratatest"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type env struct {
	t     *testing.T
	f     *stratatest.Fake
	p     *Proxy
	st    *store.Store
	url   string
	dir   string
	logs  *syncBuffer
	mu    sync.Mutex
	avail int64
}

// setup starts a fake Strata and the proxy in front of it. Idle saves are off
// unless idle > 0.
func setup(t *testing.T, idle time.Duration, maxBody int64) *env {
	t.Helper()
	e := &env{t: t, dir: t.TempDir(), logs: &syncBuffer{}, avail: 8 << 30}
	e.f = stratatest.New(e.dir)
	up := httptest.NewServer(e.f)
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	log := slog.New(slog.NewJSONHandler(e.logs, nil))
	e.st = store.New(store.Options{Dir: e.dir, MaxBytes: 64 << 30, MaxAge: time.Hour, Log: log,
		FreeSpace: func(string) (int64, error) { return 1 << 40, nil }})
	if err := e.st.Load(); err != nil {
		t.Fatal(err)
	}
	e.p = New(Options{Upstream: u, Store: e.st, MaxBody: maxBody, Metrics: NewMetrics(prometheus.NewRegistry()), Log: log,
		Policy: Policy{MinTokens: 15000, ParkMinTokens: -1, RegrowTokens: 4096, IdleDelay: idle, SnapshotBytesPerToken: 64 << 10,
			MemAvailable: func() (int64, error) { e.mu.Lock(); defer e.mu.Unlock(); return e.avail, nil }}})
	px := httptest.NewServer(e.p)
	t.Cleanup(func() { px.Close(); _ = e.p.Close(context.Background()) })
	e.url = px.URL
	return e
}

func (e *env) setAvail(n int64) { e.mu.Lock(); e.avail = n; e.mu.Unlock() }

func conversation(name string, turns int) string {
	msgs := []string{fmt.Sprintf(`{"role":"system","content":"%s"}`, name)}
	for i := 0; i < turns; i++ {
		if i > 0 {
			msgs = append(msgs, fmt.Sprintf(`{"role":"assistant","content":"answer-text-%d"}`, i))
		}
		msgs = append(msgs, fmt.Sprintf(`{"role":"user","content":"u%d"}`, i))
	}
	return `{"stream":true,"messages":[` + strings.Join(msgs, ",") + `]}`
}

// post sends a request and reads the whole answer; it returns the status and the SSE event count.
func (e *env) post(ctx context.Context, path, body string) (int, int) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+path, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0
	}
	defer func() { _ = resp.Body.Close() }()
	n := 0
	for sc := bufio.NewScanner(resp.Body); sc.Scan(); {
		if strings.HasPrefix(sc.Text(), "data: ") {
			n++
		}
	}
	return resp.StatusCode, n
}

// turn sends name's conversation at that many turns and returns the cache's action.
func (e *env) turn(name string, turns int) string {
	e.t.Helper()
	if status, events := e.post(context.Background(), "/v1/chat/completions", conversation(name, turns)); status != 200 || events != 4 {
		e.t.Fatalf("%s/%d: status %d, %d events", name, turns, status, events)
	}
	return e.last()
}

func (e *env) last() string {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	return e.p.recent[len(e.p.recent)-1].Action
}

func (e *env) files() int {
	m, _ := filepath.Glob(filepath.Join(e.dir, "dc-*.session"))
	return len(m)
}

// nameOf is the one file on disk, for following it across a turn.
func (e *env) nameOf() string {
	m, _ := filepath.Glob(filepath.Join(e.dir, "dc-*.session"))
	if len(m) != 1 {
		e.t.Fatalf("%d files on disk, want 1", len(m))
	}
	return filepath.Base(m[0])
}

func (e *env) exists(name string) bool {
	_, err := os.Stat(filepath.Join(e.dir, name))
	return err == nil
}

func saves(actions []string) (n int) {
	for _, a := range actions {
		if strings.HasPrefix(a, "save ") {
			n++
		}
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func want(t *testing.T, step, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %q, want %q", step, got, want)
	}
}

func TestSavesOnlyWhatWouldBeLost(t *testing.T) {
	e := setup(t, 0, 64<<20)
	want(t, "first request", e.turn("A", 1), actMiss)
	want(t, "next turn", e.turn("A", 2), actContinue)
	e.f.Take()

	e.f.Set(20000, 0) // A: 20000 tokens, a ~1.3 GB snapshot: it parks
	e.turn("B", 1)
	if n := saves(e.f.Take()); n != 0 {
		t.Fatalf("A parks, yet %d saves at the switch", n)
	}
	e.f.Set(20000, 1)
	want(t, "back to A", e.turn("A", 3), actRAM)

	e.f.Set(100000, 1) // 6.4 GB: over the 3.5 GiB budget, never parks
	e.turn("C", 1)
	if n := saves(e.f.Take()); n != 1 {
		t.Fatalf("too big to park: %d saves at the switch, want 1", n)
	}
	e.setAvail(2 << 30) // under the RAM floor
	e.f.Set(20000, 0)
	e.turn("D", 1)
	if n := saves(e.f.Take()); n != 1 {
		t.Fatalf("no room above the RAM floor: %d saves, want 1", n)
	}
	e.setAvail(8 << 30)

	// RAM lost everything: A comes back from disk, and D is saved first since the restore drops it
	want(t, "A from disk", e.turn("A", 4), actDisk)
	if got := e.f.Take(); len(got) != 3 || !strings.HasPrefix(got[0], "save ") || !strings.HasPrefix(got[1], "restore ") {
		t.Fatalf("A from disk: %q", got)
	}
	e.f.Set(500, 0)
	e.turn("E", 1)
	e.turn("F", 1)
	if n := saves(e.f.Take()); n != 0 {
		t.Fatal("a short conversation was saved")
	}
}

func TestIdleSave(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	e.turn("A", 2) // a quick next turn cancels the first idle save
	eventually(t, "the idle save", func() bool { return e.files() == 1 })
	if n := saves(e.f.Take()); n != 1 {
		t.Fatalf("%d idle saves, want 1", n)
	}
	e.f.Set(22000, 0) // within regrow of the file
	e.turn("A", 3)
	time.Sleep(200 * time.Millisecond)
	if n := saves(e.f.Take()); n != 0 {
		t.Fatal("saved again within regrow")
	}
	e.f.Set(30000, 0)
	e.turn("A", 4)
	eventually(t, "a new save", func() bool { return saves(e.f.Take()) == 1 })
	if e.files() != 1 {
		t.Errorf("%d files: the newer save did not supersede the older one", e.files())
	}
}

// saveAB leaves A and B both on disk (via idle saves) with B live.
func saveAB(e *env) {
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(e.t, "A's file", func() bool { return e.files() == 1 })
	e.turn("B", 1)
	eventually(e.t, "B's file", func() bool { return e.files() == 2 })
}

func TestOverlappingRequests(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	saveAB(e)

	// a request that arrives mid-restore is passed through untouched
	entered := e.f.Entered()
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 300 * time.Millisecond })
	done := make(chan int)
	go func() { s, _ := e.post(context.Background(), "/v1/chat/completions", conversation("A", 2)); done <- s }()
	want(t, "slot call", <-entered, "restore")
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 0 })
	if s, _ := e.post(context.Background(), "/v1/chat/completions", conversation("B", 2)); s != 200 {
		t.Fatalf("status %d", s)
	}
	want(t, "mid-restore", e.last(), actBusy)
	if s := <-done; s != 200 {
		t.Fatalf("restored request: status %d", s)
	}
	if e.p.cache.live != nil {
		t.Error("after overlapping requests the live conversation is still trusted")
	}

	// the same conversation from four clients at once: all answered
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay = 100 * time.Millisecond })
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			if s, _ := e.post(context.Background(), "/v1/chat/completions", conversation("C", 1)); s != 200 {
				t.Errorf("concurrent request: status %d", s)
			}
		})
	}
	wg.Wait()
}

func TestRequestDuringIdleSave(t *testing.T) {
	e := setup(t, 20*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	entered := e.f.Entered()
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 300 * time.Millisecond })
	e.turn("A", 1)
	want(t, "slot call", <-entered, "save")
	t0 := time.Now()
	want(t, "a continuing turn", e.turn("A", 2), actContinue)
	if time.Since(t0) > 250*time.Millisecond {
		t.Error("a continuing turn waited for the idle save in the proxy")
	}
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 0 })
	want(t, "a switch after the save", e.turn("B", 1), actMiss)
}

func TestClientCancel(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	saveAB(e)

	// mid-restore: the restore finishes anyway and the file stays
	entered := e.f.Entered()
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 300 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.post(ctx, "/v1/chat/completions", conversation("A", 2)); close(done) }()
	want(t, "slot call", <-entered, "restore")
	cancel()
	<-done
	e.f.Update(func(f *stratatest.Fake) { f.SlotDelay = 0 })
	eventually(t, "the restore", func() bool { return strings.Contains(e.logs.String(), `"msg":"restore"`) })
	if e.files() != 2 {
		t.Errorf("%d files after a canceled restore, want 2", e.files())
	}

	// mid-stream: the cut-off state is not trusted as live
	e.turn("A", 2)
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay = 5 * time.Second })
	ctx, cancel = context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/v1/chat/completions", strings.NewReader(conversation("A", 3)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(resp.Body).ReadString('\n')
	want(t, "the first event is not held back", line, "data: {\"i\":0}\n")
	cancel()
	_ = resp.Body.Close()
	eventually(t, "the request to end", func() bool { return strings.Contains(e.logs.String(), `"client_gone":true`) })
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay = 0 })
	if got := e.turn("A", 4); got == actContinue {
		t.Error("a cut-off answer was trusted as the live state")
	}
}

// stream sends a request, reads its answer until a data line carries stop, and closes
// the socket right there; it returns the SSE lines read.
func (e *env) stream(path, body, stop string) []string {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+path, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	var lines []string
	for sc := bufio.NewScanner(resp.Body); sc.Scan(); {
		if line := sc.Text(); strings.HasPrefix(line, "data: ") {
			lines = append(lines, line)
			if strings.Contains(line, stop) {
				break
			}
		}
	}
	cancel()
	_ = resp.Body.Close()
	return lines
}

// lastReq is the most recent request line in the log.
func (e *env) lastReq() string {
	lines := strings.Split(e.logs.String(), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.Contains(lines[i], `"msg":"request"`) {
			return lines[i]
		}
	}
	return ""
}

// A marker quoted inside streamed content is not the end of the stream, even when
// the rolling tail starts right at it.
func TestStatusWriterDone(t *testing.T) {
	rec := httptest.NewRecorder()
	s := &statusWriter{ResponseWriter: rec, status: http.StatusOK, tail: []byte{'\n'}}

	marker := "data: [DONE]"
	rest := `"}}]}` + "\n\n" // closes the JSON the marker sits inside
	chunk := `data: {"choices":[{"delta":{"content":"example: ` + marker +
		strings.Repeat("p", tailKeep-len(marker)-len(rest)) + rest
	if _, err := s.Write([]byte(chunk)); err != nil {
		t.Fatal(err)
	}
	if s.done {
		t.Fatal("a marker quoted inside content was taken for the end of the stream")
	}
	if !bytes.HasPrefix(s.tail, []byte(marker)) {
		t.Fatalf("the tail is %q, want it to start at the quoted marker", s.tail)
	}
	if _, err := s.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"next\"}}]}\n\n")); err != nil {
		t.Fatal(err)
	}
	if s.done {
		t.Fatal("the newline seeded at stream start made the quoted marker read as a stream end")
	}
	if _, err := s.Write([]byte("data: [DONE]\n\n")); err != nil {
		t.Fatal(err)
	}
	if !s.done {
		t.Fatal("the real end of the stream was not seen")
	}
}

func TestClientGoneAfterCompletion(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)

	// an agent harness that closes the socket as soon as the stream finished: the answer
	// is complete, so the conversation stays live and the idle save runs
	for i, c := range []struct{ path, body, stop string }{
		{"/v1/chat/completions", conversation("A", 1), "[DONE]"},
		{"/v1/chat/completions", conversation("B", 1), `"finish_reason":"tool_calls"`},
		{"/v1/messages", `{"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`, `"type":"message_stop"`},
		{"/v1/responses", `{"input":"hi"}`, `"type":"response.completed"`},
	} {
		e.f.Update(func(f *stratatest.Fake) { f.Finish, f.TailDelay = "", 300*time.Millisecond })
		if c.stop == `"finish_reason":"tool_calls"` {
			e.f.Update(func(f *stratatest.Fake) { f.Finish = "tool_calls" })
		}
		lines := e.stream(c.path, c.body, c.stop)
		if last := lines[len(lines)-1]; !strings.Contains(last, c.stop) {
			t.Fatalf("%s: stream ended at %q, want %s", c.path, last, c.stop)
		}
		if c.stop == `"type":"response.completed"` && len(lines[len(lines)-1]) < 1024 {
			t.Fatalf("the completed event is %d bytes: the marker must be buried by its payload", len(lines[len(lines)-1]))
		}
		eventually(t, "the request to end", func() bool {
			return strings.Contains(e.lastReq(), `"client_gone":false`)
		})
		if status, _ := e.post(context.Background(), c.path, c.body); status != 200 {
			t.Fatalf("%s: the turn after a clean close: status %d", c.path, status)
		}
		want(t, c.path+" after a clean close", e.last(), actContinue) // the state was trusted
		eventually(t, "the idle save of "+c.path, func() bool { return e.files() == i+1 })
	}

	// a cancel while the stream is still running is a mid-stream cancellation: the state
	// is forgotten, so the next turn is read fresh
	e.f.Update(func(f *stratatest.Fake) { f.Finish, f.TailDelay, f.GenDelay = "", 0, 300*time.Millisecond })
	e.stream("/v1/chat/completions", conversation("G", 1), `"i":0`)
	eventually(t, "the canceled request to end", func() bool {
		return strings.Contains(e.lastReq(), `"client_gone":true`)
	})
	e.post(context.Background(), "/v1/chat/completions", conversation("G", 2))
	want(t, "the turn after a mid-stream cancel", e.last(), actMiss)
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay = 0 })

	// a model quoting "data: [DONE]" inside the answer is not the end of the stream
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay, f.Content = 300*time.Millisecond, "talking about data: [DONE]" })
	lines := e.stream("/v1/chat/completions", conversation("H", 1), "data: [DONE]")
	if last := lines[len(lines)-1]; !strings.Contains(last, `"content"`) {
		t.Fatalf("the marker was not streamed inside content: %q", last)
	}
	eventually(t, "the canceled request to end", func() bool {
		return strings.Contains(e.lastReq(), `"client_gone":true`)
	})
	e.post(context.Background(), "/v1/chat/completions", conversation("H", 2))
	want(t, "the turn after a cancel at a quoted marker", e.last(), actMiss)
	e.f.Update(func(f *stratatest.Fake) { f.GenDelay, f.Content = 0, "" })
}

func TestColdEngineWakesAndRestores(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	// the engine unloaded (--lazy, idle_unload_s): /metrics has no context or KV type,
	// and the slot API answers 503 "not loaded"
	e.f.Take()
	e.f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded = true, false })
	want(t, "a cold engine with the file on disk", e.turn("A", 2), actDisk)
	if !strings.Contains(e.logs.String(), `"msg":"waking engine for restore"`) {
		t.Error("the engine was not woken for the restore")
	}
	if acts := e.f.Take(); len(acts) != 4 || !strings.HasPrefix(acts[0], "restore ") ||
		acts[1] != "gen /v1/chat/completions" || !strings.HasPrefix(acts[2], "restore ") ||
		acts[3] != "gen /v1/chat/completions" {
		t.Errorf("actions %q: want the refused restore, the wakeup, the restore again, the turn", acts)
	}
	if e.files() != 1 {
		t.Errorf("%d files: a restore on a woken engine evicted them", e.files())
	}
	want(t, "the turn after the restore", e.turn("A", 3), actContinue)
}

func TestColdEngineNoFilePassesThrough(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	e.f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded = true, false })
	want(t, "a cold engine with no files", e.turn("A", 1), actMiss)
	if !strings.Contains(e.logs.String(), `"reason":"engine_unloaded_cold"`) {
		t.Error("a cold engine with nothing to restore was not recorded as engine_unloaded_cold")
	}
	if acts := e.f.Take(); len(acts) != 1 || acts[0] != "gen /v1/chat/completions" {
		t.Errorf("actions %q: a cold engine with nothing to restore must not be woken", acts)
	}
}

func TestUnloadedEngineKeepsFiles(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	// /metrics of an unloaded engine has no fingerprint: it must not read as one, and
	// the files of the loaded engine must stay
	e.f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded = true, false })
	aFile := e.nameOf()
	want(t, "a switch while unloaded", e.turn("B", 1), actMiss)
	if e.files() != 1 {
		t.Fatalf("%d files after a request against an unloaded engine, want 1", e.files())
	}
	if strings.Contains(e.logs.String(), `"reason":"fingerprint"`) {
		t.Error("files were evicted for a fingerprint read off an unloaded engine")
	}
	want(t, "back to A", e.turn("A", 2), actDisk)
	if !e.exists(aFile) {
		t.Errorf("A's file %s went to a fingerprint read off an unloaded engine", aFile)
	}
	if e.files() != 2 { // A's file, plus B's idle save
		t.Errorf("%d files after the restore, want 2", e.files())
	}
}

func TestWokenEngineWithNewFingerprint(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	// woken into a different model: the fingerprint is checked before the restore is retried
	e.f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded, f.Model = true, false, "another-model" })
	want(t, "woken into another model", e.turn("A", 2), actMiss)
	if !strings.Contains(e.logs.String(), `"msg":"woken into another engine; reading fresh"`) {
		t.Error("the fingerprint check did not say what happened")
	}
	if e.files() != 0 {
		t.Errorf("%d files: a fingerprint change kept the files of the old engine", e.files())
	}
}

func TestWakeupFailurePropagates(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	// the engine refuses to wake: the failure the log and the metric see is the refused
	// request, not the initial 503 "not loaded"
	e.f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded, f.GenStatus = true, false, 502 })
	if status, _ := e.post(context.Background(), "/v1/chat/completions", conversation("A", 2)); status != 502 {
		t.Fatalf("a turn that cannot wake the engine: status %d", status)
	}
	want(t, "the restore that woke nothing", e.last(), actMiss)
	if !strings.Contains(e.logs.String(), `"msg":"waking engine for restore"`) {
		t.Error("the restore did not try to wake the engine")
	}
	if !strings.Contains(e.logs.String(), `"reason":"http_502"`) {
		t.Error("the failure was not attributed to the refused wakeup")
	}
	if strings.Contains(e.logs.String(), `"reason":"http_503"`) {
		t.Error("the initial 503 was reported as the restore failure")
	}
	if e.files() != 1 {
		t.Errorf("%d files: a failed wakeup evicted the file", e.files())
	}
}

func TestTruncatedSessionFile(t *testing.T) {
	e := setup(t, 50*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	// the file on disk restores far fewer tokens than its name claims
	e.f.Update(func(f *stratatest.Fake) { f.Started++; f.Tokens = 8000 })
	want(t, "restoring a truncated file", e.turn("A", 2), actDisk)
	if !strings.Contains(e.logs.String(), `"msg":"truncated session file; forgetting"`) {
		t.Error("a truncated file was kept")
	}
	if e.files() != 0 {
		t.Errorf("%d files: a truncated file was kept", e.files())
	}
	// with the broken file gone, the conversation is saved afresh instead of being
	// skipped as already on disk
	e.f.Set(20000, 0)
	e.turn("A", 3)
	eventually(t, "the save of the regrown conversation", func() bool { return e.files() == 1 })
}

func TestStrataRestartAndFingerprint(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	e.f.Set(20000, 0)
	e.turn("A", 1)
	eventually(t, "A's file", func() bool { return e.files() == 1 })

	e.f.Update(func(f *stratatest.Fake) { f.Started++ }) // the server restarted: A is no longer live
	want(t, "after a restart", e.turn("A", 2), actDisk)

	e.f.Set(20000, 1)
	e.turn("B", 1)
	e.f.Update(func(f *stratatest.Fake) { f.Loaded = false })
	if got := e.turn("A", 3); got == actRAM {
		t.Error("an unloaded engine was believed to have A in RAM")
	}

	e.f.Update(func(f *stratatest.Fake) { f.Loaded = true; f.Model = "another-model" }) // new fingerprint
	e.turn("C", 1)
	if e.files() != 0 {
		t.Errorf("%d files of the old model left", e.files())
	}
}

func TestRestoreFailure(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	saveAB(e)
	e.f.Update(func(f *stratatest.Fake) { f.SlotStatus, f.SlotKind = 507, "memory" })
	want(t, "no RAM to restore", e.turn("A", 2), actMiss)
	if e.files() != 2 {
		t.Error("a file Strata had no RAM for was deleted")
	}
	e.f.Update(func(f *stratatest.Fake) { f.SlotKind = "invalid" })
	e.turn("B", 2)
	if e.files() != 1 {
		t.Error("a file Strata rejects was kept")
	}
}

func TestFailOpen(t *testing.T) {
	e := setup(t, 0, 1<<20)

	// Strata down: its own error reaches the client
	e.f.Update(func(f *stratatest.Fake) { f.Down = true })
	if s, _ := e.post(context.Background(), "/v1/chat/completions", conversation("A", 1)); s != http.StatusServiceUnavailable {
		t.Errorf("Strata down: status %d", s)
	}
	want(t, "Strata down", e.last(), actPass)
	e.f.Update(func(f *stratatest.Fake) { f.Down = false })

	// malformed JSON: passed through, Strata answers
	if s, _ := e.post(context.Background(), "/v1/chat/completions", `{"messages": [`); s != http.StatusBadRequest {
		t.Errorf("malformed JSON: status %d", s)
	}
	want(t, "malformed JSON", e.last(), actUnkeyed)

	// a body over the limit streams through whole, unbuffered
	big := `{"messages":[{"role":"user","content":"` + strings.Repeat("x", 2<<20) + `"}]}`
	if s, _ := e.post(context.Background(), "/v1/chat/completions", big); s != 200 {
		t.Errorf("big body: status %d", s)
	}
	want(t, "big body", e.last(), actPass)
	e.f.Update(func(f *stratatest.Fake) {
		if f.Bodies[len(f.Bodies)-1] != big {
			t.Error("Strata did not get the whole big body")
		}
	})

	// other endpoints pass through with the client's Host, and make the live conversation unknown
	e.turn("A", 1)
	want(t, "next turn", e.turn("A", 2), actContinue)
	req, _ := http.NewRequest(http.MethodPost, e.url+"/v1/completions", strings.NewReader(`{"prompt":"x"}`))
	req.Host = "llm.example:8080"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	want(t, "pass-through", string(b), "host=llm.example:8080 path=/v1/completions")
	if got := e.turn("A", 3); got == actContinue {
		t.Error("a raw completion did not make the live conversation unknown")
	}

	// a panic in the cache logic: the request still goes through
	e.p.cache.files = nil
	e.f.Set(20000, 0)
	if s, _ := e.post(context.Background(), "/v1/chat/completions", conversation("Z", 1)); s != 200 {
		t.Errorf("after a panic: status %d", s)
	}
	want(t, "panic", e.last(), actError)
	if e.p.cache.inflight != 0 {
		t.Errorf("inflight %d after a panic", e.p.cache.inflight)
	}
}

func TestDialectsAndNoContentInLogs(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 64<<20)
	if s, n := e.post(context.Background(), "/v1/messages", `{"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`); s != 200 || n != 6 {
		t.Errorf("/v1/messages: status %d, %d events", s, n)
	}
	if s, n := e.post(context.Background(), "/v1/responses", `{"input":"hi"}`); s != 200 || n != 5 {
		t.Errorf("/v1/responses: status %d, %d events", s, n)
	}
	secret := "the-launch-code-is-4471"
	e.f.Set(20000, 0)
	e.post(context.Background(), "/v1/chat/completions", strings.ReplaceAll(conversation("A", 1), "u0", secret))
	eventually(t, "A's file", func() bool { return e.files() == 1 })
	e.turn("B", 1)
	e.post(context.Background(), "/v1/chat/completions", strings.ReplaceAll(conversation("A", 2), "u0", secret))
	e.post(context.Background(), "/v1/chat/completions", `{"messages":[{"role":"user","content":"`+secret+`"`)

	logs := e.logs.String()
	for _, msg := range []string{`"msg":"request"`, `"msg":"save"`, `"msg":"restore"`} {
		if !strings.Contains(logs, msg) {
			t.Errorf("no %s line", msg)
		}
	}
	if strings.Contains(logs, secret) || strings.Contains(logs, "answer-text") {
		t.Error("message content reached the logs")
	}
}

func TestKeys(t *testing.T) {
	var k keyer
	key := func(path, body string) []string {
		t.Helper()
		keys, ok := k.keys(path, []byte(body))
		if !ok {
			t.Fatalf("not keyed: %s", body)
		}
		return keys
	}
	const chat = "/v1/chat/completions"
	turn1 := key(chat, `{"tools":[{"type":"function"}],"messages":[{"role":"system","content":"s"},{"role":"user","content":"hi"}]}`)
	// other sampling, key order, whitespace, a moved cache_control
	turn2 := key(chat, `{"temperature":0.2,"tools":[{"type":"function"}],"messages":[ {"content":"s","role":"system"},
		{"role":"user","content":"hi","cache_control":{"type":"ephemeral"}},{"role":"assistant","content":"x"},{"role":"user","content":"y"}]}`)
	if !extends(turn2, turn1) || extends(turn1, turn2) || !extends(turn1, turn1) {
		t.Error("turn 2 should extend turn 1, not the reverse; a retry extends itself")
	}
	msgs := `"messages":[{"role":"user","content":"hi"}]`
	base := key(chat, `{`+msgs+`}`)
	for _, other := range [][]string{
		key(chat, `{"tools":[{"type":"function"}],`+msgs+`}`),
		key(chat, `{"reasoning_effort":"high",`+msgs+`}`),
		key("/v1/messages", `{`+msgs+`}`),
		key("/v1/messages", `{"system":"be brief",`+msgs+`}`),
		key(chat, `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}`),
	} {
		if extends(other, base) {
			t.Errorf("%v extends the base conversation", other)
		}
	}
	for _, b := range []struct{ path, body string }{
		{chat, `not json`}, {chat, `{}`}, {chat, `{"messages":[]}`}, {chat, `{"messages":"nope"}`},
		{"/v1/responses", `{"input":[{"role":"user","content":"x"}],"previous_response_id":"r1"}`},
	} {
		if _, ok := k.keys(b.path, []byte(b.body)); ok {
			t.Errorf("keyed: %s", b.body)
		}
	}
	if keys := key("/v1/responses", `{"input":"hello"}`); len(keys) != 1 {
		t.Error("string input")
	}
}
