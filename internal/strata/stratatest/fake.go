// Package stratatest is a fake Strata server for tests: one live
// conversation, a parked count and engine identity the test sets, session
// files written for real, and SSE answers on both dialects.
package stratatest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Fake is the server's state. Set its fields under Lock, or use the setters.
type Fake struct {
	Dir string // the slot_save_path

	mu         sync.Mutex
	Tokens     int // last_timings total
	Parked     int
	Started    int64
	Loaded     bool
	Model      string
	BudgetMiB  int64
	MinTokens  int
	SlotStatus int           // non-zero: slot calls answer this status
	SlotKind   string        // with this kind
	SlotDelay  time.Duration // save/restore take this long
	GenDelay   time.Duration // a pause after the first event
	Down       bool          // every request answers 503
	actions    []string      // "save NAME", "restore NAME", "gen PATH", "other METHOD PATH"
	Bodies     []string      // the bodies of generation requests, in order
	entered    chan string   // when set, each slot call reports itself here first
}

// New returns a fake with a 3.5 GiB, 4-slot RAM cache that parks
// conversations of at least 15000 tokens.
func New(dir string) *Fake {
	return &Fake{Dir: dir, Started: 1000, Loaded: true, Model: "test-model", BudgetMiB: 3584, MinTokens: 15000}
}

// Set sets the live token count and the parked count.
func (f *Fake) Set(tokens, parked int) {
	f.mu.Lock()
	f.Tokens, f.Parked = tokens, parked
	f.mu.Unlock()
}

// Update runs fn with the state locked.
func (f *Fake) Update(fn func(f *Fake)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

// Take returns and clears the actions seen so far.
func (f *Fake) Take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.actions
	f.actions = nil
	return a
}

// Entered returns a channel that receives "save" or "restore" as each slot call starts.
func (f *Fake) Entered() <-chan string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.entered == nil {
		f.entered = make(chan string, 16)
	}
	return f.entered
}

func (f *Fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	if f.Down {
		f.mu.Unlock()
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.URL.Path == "/v1/status":
		fmt.Fprintf(w, `{"started":%d,"loaded":%t,"last_timings":{"cache_n":0,"prompt_n":%d,"predicted_n":0}}`,
			f.Started, f.Loaded, f.Tokens)
		f.mu.Unlock()
	case r.URL.Path == "/metrics":
		fmt.Fprintf(w, `{"conversation_cache":{"parked":%d,"bytes":0,"budget_mib":%d},
			"engine":{"model":%q,"version":"0.1.45","context":262144,"kv":"k8v4","cvec":null,
			"conversation_cache_min_free_mib":2048,"conversation_cache_min_tokens":%d}}`,
			f.Parked, f.BudgetMiB, f.Model, f.MinTokens)
		f.mu.Unlock()
	case strings.HasPrefix(r.URL.Path, "/slots/0"):
		f.slot(w, r) // unlocks
	case r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/messages" || r.URL.Path == "/v1/responses":
		f.gen(w, r) // unlocks
	default:
		f.actions = append(f.actions, "other "+r.Method+" "+r.URL.Path)
		f.mu.Unlock()
		fmt.Fprintf(w, "host=%s path=%s", r.Host, r.URL.Path)
	}
}

func (f *Fake) slot(w http.ResponseWriter, r *http.Request) {
	var b struct{ Filename string }
	_ = json.NewDecoder(r.Body).Decode(&b)
	action := r.URL.Query().Get("action")
	f.actions = append(f.actions, action+" "+b.Filename)
	status, kind, delay, tokens, entered := f.SlotStatus, f.SlotKind, f.SlotDelay, f.Tokens, f.entered
	f.mu.Unlock()
	if entered != nil {
		entered <- action
	}
	time.Sleep(delay)
	if status != 0 {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"kind":%q,"message":"refused"}}`, kind)
		return
	}
	p := filepath.Join(f.Dir, b.Filename)
	if action == "save" {
		_ = os.WriteFile(p, make([]byte, 1000), 0o600)
		fmt.Fprintf(w, `{"n_saved":%d,"n_written":1000}`, tokens)
		return
	}
	if _, err := os.Stat(p); err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error":{"message":"no saved session"}}`)
		return
	}
	fmt.Fprint(w, `{"n_restored":20000,"n_read":1000}`)
}

func (f *Fake) gen(w http.ResponseWriter, r *http.Request) {
	f.actions = append(f.actions, "gen "+r.URL.Path)
	delay := f.GenDelay
	f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.Bodies = append(f.Bodies, string(body))
	f.mu.Unlock()
	if !json.Valid(body) {
		http.Error(w, `{"error":{"message":"invalid JSON"}}`, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	if r.URL.Path == "/v1/messages" {
		for _, ev := range []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"} {
			fmt.Fprintf(w, "event: %s\ndata: {}\n\n", ev)
			fl.Flush()
			if ev == "message_start" {
				sleep(r, delay)
			}
		}
		return
	}
	for i := 0; i < 3; i++ {
		fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i)
		fl.Flush()
		if i == 0 {
			sleep(r, delay)
		}
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func sleep(r *http.Request, d time.Duration) {
	select {
	case <-time.After(d):
	case <-r.Context().Done():
	}
}
