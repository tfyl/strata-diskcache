// Package strata is a client for the Strata endpoints the cache uses: the
// slot API (POST /slots/0?action=save|restore), /v1/status and the JSON
// /metrics.
package strata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Client talks to one Strata server.
type Client struct {
	base string
	http http.Client
}

// New returns a client for the Strata server at base, e.g. http://127.0.0.1:8081.
func New(base string) *Client { return &Client{base: strings.TrimRight(base, "/")} }

// SlotError is Strata refusing or failing a save or restore.
type SlotError struct {
	Status  int
	Kind    string // Strata's own: invalid, storage, memory or io (may be empty)
	Message string
}

func (e *SlotError) Error() string {
	return fmt.Sprintf("strata: HTTP %d %s: %s", e.Status, e.Kind, e.Message)
}

// ErrorKind names an error for metrics and logs: not_found, Strata's kind,
// http_<status>, canceled or transport.
func ErrorKind(err error) string {
	var se *SlotError
	switch {
	case errors.As(err, &se) && se.Status == http.StatusNotFound:
		return "not_found"
	case errors.As(err, &se) && se.Kind != "":
		return se.Kind
	case errors.As(err, &se):
		return fmt.Sprintf("http_%d", se.Status)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "transport"
	}
}

// Save writes the live conversation to name in Strata's slot_save_path and
// returns the tokens and bytes written. Strata runs it in its request queue,
// between requests.
func (c *Client) Save(ctx context.Context, name string) (tokens int, bytes int64, err error) {
	return c.slot(ctx, "save", name)
}

// Restore loads name as the live conversation. Strata drops the live one
// without parking it.
func (c *Client) Restore(ctx context.Context, name string) (tokens int, bytes int64, err error) {
	return c.slot(ctx, "restore", name)
}

func (c *Client) slot(ctx context.Context, action, name string) (int, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute) // a 200K-token file is several GB
	defer cancel()
	body, _ := json.Marshal(map[string]string{"filename": name})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/slots/0?action="+action, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		var eb struct {
			Error struct{ Kind, Message string } `json:"error"`
		}
		_ = json.Unmarshal(raw, &eb)
		return 0, 0, &SlotError{Status: resp.StatusCode, Kind: eb.Error.Kind, Message: eb.Error.Message}
	}
	var ok struct {
		Saved    int   `json:"n_saved"`
		Written  int64 `json:"n_written"`
		Restored int   `json:"n_restored"`
		Read     int64 `json:"n_read"`
	}
	if err := json.Unmarshal(raw, &ok); err != nil {
		return 0, 0, fmt.Errorf("strata: %s answer: %w", action, err)
	}
	return ok.Saved + ok.Restored, ok.Written + ok.Read, nil
}

func (c *Client) getJSON(ctx context.Context, path string, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("strata: GET %s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// Status is the server's state from /v1/status.
type Status struct {
	Started    int64 // when the server started (unix seconds)
	Loaded     bool  // the engine is up; an unloaded engine has no RAM cache
	LiveTokens int   // tokens the engine holds for the last request's conversation
}

// Status reads /v1/status.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var st struct {
		Started     int64 `json:"started"`
		Loaded      bool  `json:"loaded"`
		LastTimings *struct {
			CacheN     int `json:"cache_n"`
			PromptN    int `json:"prompt_n"`
			PredictedN int `json:"predicted_n"`
		} `json:"last_timings"`
	}
	if err := c.getJSON(ctx, "/v1/status", &st); err != nil {
		return Status{}, err
	}
	out := Status{Started: st.Started, Loaded: st.Loaded}
	if t := st.LastTimings; t != nil {
		out.LiveTokens = t.CacheN + t.PromptN + t.PredictedN
	}
	return out, nil
}

// Engine is the engine's identity and its in-RAM conversation cache.
type Engine struct {
	Fingerprint string // model, version, context, KV type, control vector: what a session file is tied to
	Parked      int    // conversations parked in RAM now
	ParkedBytes int64  // their snapshots' total
	BudgetMiB   int64  // --conversation-cache-mib (0: no RAM cache)
	MinFreeMiB  int64  // --conversation-cache-min-free-mib
	MinTokens   int    // --conversation-cache-min-tokens
}

// Engine reads the JSON /metrics.
func (c *Client) Engine(ctx context.Context) (Engine, error) {
	var m struct {
		CC struct {
			Parked    *int  `json:"parked"`
			Bytes     int64 `json:"bytes"`
			BudgetMiB int64 `json:"budget_mib"`
		} `json:"conversation_cache"`
		Engine struct {
			Model            string `json:"model"`
			Version, Context any
			KV               any   `json:"kv"`
			CVec             any   `json:"cvec"`
			MinFreeMiB       int64 `json:"conversation_cache_min_free_mib"`
			MinTokens        int   `json:"conversation_cache_min_tokens"`
		} `json:"engine"`
	}
	if err := c.getJSON(ctx, "/metrics", &m); err != nil {
		return Engine{}, err
	}
	if m.CC.Parked == nil || m.Engine.Model == "" {
		return Engine{}, errors.New("strata: /metrics has no conversation_cache.parked or engine.model")
	}
	e := m.Engine
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%v|%v|%v|%v", e.Model, e.Version, e.Context, e.KV, e.CVec))
	return Engine{Fingerprint: hex.EncodeToString(sum[:6]), Parked: *m.CC.Parked, ParkedBytes: m.CC.Bytes,
		BudgetMiB: m.CC.BudgetMiB, MinFreeMiB: e.MinFreeMiB, MinTokens: e.MinTokens}, nil
}

// MemAvailable is MemAvailable from /proc/meminfo in bytes: what this machine
// (in a container, the container) can still give out. Strata's park checks
// read the same figure.
func MemAvailable() (int64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemAvailable(string(b))
}

func parseMemAvailable(meminfo string) (int64, error) {
	for line := range strings.Lines(meminfo) {
		if rest, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			return kb << 10, err
		}
	}
	return 0, errors.New("strata: /proc/meminfo has no MemAvailable")
}
