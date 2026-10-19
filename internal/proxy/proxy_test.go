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
	if s, n := e.post(context.Background(), "/v1/responses", `{"input":"hi"}`); s != 200 || n != 4 {
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
