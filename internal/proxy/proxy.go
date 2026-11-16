// Package proxy is a transparent reverse proxy to Strata with an on-disk
// conversation cache: before each generation request it restores the
// conversation's session file if Strata no longer has it in RAM, and it saves
// conversations that would otherwise be lost.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/tfyl/strata-diskcache/internal/store"
	"github.com/tfyl/strata-diskcache/internal/strata"
)

// Options configure the proxy.
type Options struct {
	Upstream *url.URL
	Store    *store.Store // nil: a plain proxy, no disk cache
	Policy   Policy
	MaxBody  int64 // generation bodies larger than this are streamed through unbuffered
	Metrics  *Metrics
	Log      *slog.Logger
}

// Proxy is the client-facing http.Handler.
type Proxy struct {
	rp      *httputil.ReverseProxy
	cache   *cache // nil without a store
	maxBody int64
	log     *slog.Logger

	mu     sync.Mutex
	recent []record // the last generation requests, for /requests
}

// New returns a proxy to opt.Upstream.
func New(opt Options) *Proxy {
	p := &Proxy{maxBody: opt.MaxBody, log: opt.Log}
	if opt.Store != nil {
		p.cache = newCache(strata.New(opt.Upstream.String()), opt.Store, opt.Metrics, opt.Log, opt.Policy)
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(opt.Upstream)
			pr.Out.Host = pr.In.Host // Strata's Host and Origin checks see the client's name
			for _, h := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v, ok := pr.In.Header[h]; ok { // Rewrite drops them; a client behind another proxy keeps them
					pr.Out.Header[h] = v
				}
			}
		},
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true, // the client's Accept-Encoding goes through as it is
		},
		FlushInterval: -1, // SSE: every chunk as it comes
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return // the client went away; nobody to answer
			}
			p.log.Warn("upstream", "method", r.Method, "path", r.URL.Path, "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":{"type":"server_error","code":502,"message":"Strata is not answering (starting or restarting?)"}}`)
		},
	}
	return p
}

// Close stops the cache, waiting for a save under way until ctx ends.
func (p *Proxy) Close(ctx context.Context) error {
	if p.cache == nil {
		return nil
	}
	return p.cache.close(ctx)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	gen := r.Method == http.MethodPost &&
		(r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/messages" || r.URL.Path == "/v1/responses")
	if !gen || p.cache == nil {
		if p.cache != nil && r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			p.cache.invalidate() // /v1/completions, /slots, /load ...: the live conversation may have changed
		}
		p.rp.ServeHTTP(w, r)
		return
	}

	start := time.Now()
	t := &ticket{action: actPass, reason: "body_too_large"}
	body, err := io.ReadAll(io.LimitReader(r.Body, p.maxBody+1))
	switch {
	case err != nil:
		http.Error(w, "reading the request body: "+err.Error(), http.StatusBadRequest)
		return
	case int64(len(body)) > p.maxBody: // stream it through as it is, unbuffered from here
		r.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
		p.cache.invalidate()
	default:
		r.Body, r.ContentLength, r.TransferEncoding = io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
		t = p.cache.prepare(r.Context(), r.URL.Path, body)
	}
	prepared := time.Since(start)
	client, _, _ := net.SplitHostPort(r.RemoteAddr)
	p.remember(record{At: float64(time.Now().UnixMicro()) / 1e6, Path: r.URL.Path, Client: client,
		UserAgent: r.UserAgent(), Action: t.action})

	// tail starts with a newline so the first chunk can be seen as a line start
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK, tail: []byte{'\n'}}
	defer func() {
		gone := r.Context().Err() != nil && !sw.done
		p.cache.finish(t, sw.status, gone)
		// one line per request: what the cache decided, never what was said
		p.log.Info("request", "path", r.URL.Path, "client", client, "action", t.action, "reason", t.reason,
			"tokens", t.tokens, "prepare_ms", prepared.Milliseconds(), "status", sw.status, "client_gone", gone,
			"ms", time.Since(start).Milliseconds())
	}()
	p.rp.ServeHTTP(sw, r)
}

// statusWriter records the status; Unwrap keeps Flush (SSE) and hijacking.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
	done   bool   // the stream reached its terminal event
	tail   []byte // the last bytes written, seeded with a newline at stream start
}

// tailKeep is what Write remembers across calls: enough to see a marker split
// between two writes.
const tailKeep = 64

func (s *statusWriter) WriteHeader(code int) {
	if !s.wrote && code >= 200 {
		s.status, s.wrote = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	s.wrote = true
	if !s.done && len(b) > 0 {
		scanned := make([]byte, 0, len(s.tail)+len(b))
		scanned = append(scanned, s.tail...)
		scanned = append(scanned, b...)
		for _, m := range doneMarkers {
			if bytes.Contains(scanned, m) {
				s.done = true
				s.tail = nil
				break
			}
		}
		if !s.done {
			if len(scanned) > tailKeep {
				s.tail = append(s.tail[:0], scanned[len(scanned)-tailKeep:]...)
			} else {
				s.tail = scanned
			}
		}
	}
	return s.ResponseWriter.Write(b)
}

// doneMarkers are the events that close a stream in the three dialects; none of
// them appears mid-stream. A marker is searched in the whole chunk, because the
// Responses API writes kilobytes after "response.completed". [DONE] is anchored
// to its line: inside JSON strings newlines are escaped, so a model quoting
// "data: [DONE]" in streamed content is not the end of the stream.
var doneMarkers = [][]byte{
	[]byte("\ndata: [DONE]"),
	[]byte(`"finish_reason":"stop"`), []byte(`"finish_reason": "stop"`),
	[]byte(`"finish_reason":"tool_calls"`), []byte(`"finish_reason": "tool_calls"`),
	[]byte(`"finish_reason":"length"`), []byte(`"finish_reason": "length"`),
	[]byte(`"type":"message_stop"`), []byte(`"type": "message_stop"`),
	[]byte(`"type":"response.completed"`), []byte(`"type": "response.completed"`),
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// record is one generation request as the proxy saw it. Strata sees only the
// proxy's address, so a tracer that reads Strata's request log can join these
// on by path and start time.
type record struct {
	At        float64 `json:"at"` // unix seconds, when it was forwarded
	Path      string  `json:"path"`
	Client    string  `json:"client"`
	UserAgent string  `json:"user_agent"`
	Action    string  `json:"diskcache"`
}

func (p *Proxy) remember(r record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recent = append(p.recent, r)
	if len(p.recent) > 256 {
		p.recent = append([]record(nil), p.recent[len(p.recent)-256:]...)
	}
}

// Requests serves the last 256 generation requests as JSON.
func (p *Proxy) Requests(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	recs := append([]record(nil), p.recent...)
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"requests": recs})
}
