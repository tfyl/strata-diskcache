package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tfyl/strata-diskcache/internal/store"
	"github.com/tfyl/strata-diskcache/internal/strata"
)

// Policy tunes when the cache saves.
type Policy struct {
	MinTokens     int           // save conversations of at least this many tokens
	ParkMinTokens int           // the engine parks only these; -1 asks the engine
	RegrowTokens  int           // save again once a conversation has grown this much past its file
	IdleDelay     time.Duration // quiet time after a turn before it is saved; 0: never
	// RAM per token of a parked conversation's snapshot, to predict a refused park
	SnapshotBytesPerToken int64
	MemAvailable          func() (int64, error) // default strata.MemAvailable
}

// Actions, one per generation request.
const (
	actContinue = "continue" // continues the live conversation
	actRAM      = "ram"      // switches to a conversation parked in RAM
	actDisk     = "disk"     // switches to a conversation restored from its file
	actMiss     = "miss"     // switches to a conversation with no copy anywhere
	actBusy     = "busy"     // other requests in flight: passed through untouched
	actUnkeyed  = "unkeyed"  // a body the cache can't key
	actPass     = "pass"     // Strata didn't answer, or the body was too big to look at
	actError    = "error"    // the cache's own logic failed
)

// cache decides, per generation request, what Strata needs before it. Strata
// can save only the live conversation (a parked one is out of reach), and its
// restore drops the live one without parking it. So: a continuing turn passes
// straight through; a switch restores the incoming conversation's file unless
// it is parked in RAM; the outgoing one is saved at the switch only when it
// would be lost there, and otherwise while idle. See docs/design.md.
type cache struct {
	eng   *strata.Client
	files *store.Store
	m     *Metrics
	log   *slog.Logger
	pol   Policy
	keys  keyer

	ctx    context.Context // ends with close; idle saves run under it
	cancel context.CancelFunc
	wg     sync.WaitGroup

	saveMu sync.Mutex // one switch decision or idle save at a time

	mu          sync.Mutex
	inflight    int
	seq         uint64
	live        []string   // keys of the conversation the engine holds; nil: unknown
	ram         [][]string // believed parked in RAM, most recent first
	bpt         int64      // file bytes per token of the last save
	fingerprint string
	started     int64
	idle        *time.Timer
}

// ticket is one request's decision.
type ticket struct {
	action, reason string
	tokens         int // restored, or the outgoing conversation's
	seq            uint64
	counted        bool
	keys           []string
	prev           []string // the live conversation this request switched away from
	prevParks      bool
}

func newCache(eng *strata.Client, files *store.Store, m *Metrics, log *slog.Logger, pol Policy) *cache {
	if pol.MemAvailable == nil {
		pol.MemAvailable = strata.MemAvailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &cache{eng: eng, files: files, m: m, log: log, pol: pol, ctx: ctx, cancel: cancel}
}

// close stops idle saves and waits for one under way until ctx ends. A save
// Strata is still writing completes on its own; the next start indexes it.
func (c *cache) close(ctx context.Context) error {
	c.mu.Lock()
	c.cancel() // under mu: no turn ending now can schedule another idle save
	if c.idle != nil && c.idle.Stop() {
		c.wg.Done()
	}
	c.mu.Unlock()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("a save was still running: %w", ctx.Err())
	}
}

// prepare runs before a generation request is forwarded. Whatever happens in
// it, the request goes on: errors and panics only cost the cache.
func (c *cache) prepare(ctx context.Context, path string, body []byte) (t *ticket) {
	t = &ticket{action: actError}
	defer func() {
		if v := recover(); v != nil {
			c.log.Error("cache panic; request passed through", "path", path, "panic", fmt.Sprint(v))
			c.m.panics.Inc()
			t.action, t.reason = actError, "panic"
		}
	}()
	keys, keyed := c.keys.keys(path, body)
	c.mu.Lock()
	c.inflight++
	c.seq++
	t.seq, t.counted, t.keys = c.seq, true, keys
	busy, live := c.inflight > 1, c.live
	c.mu.Unlock()

	switch {
	case busy:
		t.action, t.reason = actBusy, "concurrent"
		return t
	case !keyed:
		t.action, t.reason = actUnkeyed, "no_messages"
		return t
	}
	st, err := c.eng.Status(ctx)
	if err != nil {
		t.action, t.reason = actPass, "status_"+strata.ErrorKind(err)
		return t
	}
	if c.restarted(st) {
		live = nil
	}
	if live != nil && extends(keys, live) {
		t.action, t.reason = actContinue, "live"
		return t
	}

	// A switch. An idle save under way finishes first; Strata would queue this
	// request behind it anyway.
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	var fp string
	en, err := c.eng.Engine(ctx)
	switch {
	case err == nil:
		fp = c.learn(en)
	case !st.Loaded:
		// An unloaded engine has no /metrics to read a fingerprint from. The last one we
		// learned, or the newest file on disk, is what a restore has to match; a cold start
		// with no files has nothing to restore.
		c.mu.Lock()
		fp = c.fingerprint
		c.mu.Unlock()
		if fp == "" {
			fp = c.files.AnyFingerprint()
		}
		if fp == "" {
			t.action, t.reason = actMiss, "engine_unloaded_cold"
			return t
		}
	default:
		t.action, t.reason = actMiss, "engine_"+strata.ErrorKind(err)
		return t
	}
	inRAM := c.inRAM(keys)
	var file store.Entry
	var onDisk bool
	if !inRAM {
		file, onDisk = c.files.Lookup(fp, keys)
	}
	if live != nil {
		t.prev, t.tokens = live, st.LiveTokens
		t.prevParks = c.willPark(st.LiveTokens, en)
		if t.prevParks && !onDisk {
			c.skip("parks", st.LiveTokens)
		} else {
			c.save(ctx, fp, live, st.LiveTokens, "switch")
		}
	}
	switch {
	case inRAM:
		t.action, t.reason = actRAM, "parked"
	case !onDisk:
		t.action, t.reason = actMiss, "no_copy"
	case c.restore(ctx, file, t):
		t.action, t.reason = actDisk, "restored"
		t.prevParks = false // a restore drops the outgoing conversation
	default:
		t.action, t.reason = actMiss, "restore_failed"
	}
	return t
}

// invalidate forgets the live conversation: a request the cache didn't see
// into (another endpoint, an oversized body) may have changed it.
func (c *cache) invalidate() {
	c.mu.Lock()
	c.live = nil
	c.mu.Unlock()
}

// restarted notes a server restart or an unloaded engine: the RAM cache and
// the live conversation are gone.
func (c *cache) restarted(st strata.Status) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	gone := (c.started != 0 && st.Started != c.started) || !st.Loaded
	c.started = st.Started
	if gone {
		c.live, c.ram = nil, nil
	}
	return gone
}

// learn records the engine's fingerprint and how many conversations it has
// parked. A new fingerprint makes every file of another one useless.
func (c *cache) learn(en strata.Engine) string {
	c.mu.Lock()
	changed := en.Fingerprint != c.fingerprint
	c.fingerprint = en.Fingerprint
	if changed {
		c.ram = nil
	}
	c.ram = c.ram[:min(len(c.ram), en.Parked)] // the engine evicts the oldest first
	c.mu.Unlock()
	if changed {
		c.log.Info("engine fingerprint", "fingerprint", en.Fingerprint)
		c.files.KeepFingerprint(en.Fingerprint)
	}
	return en.Fingerprint
}

func (c *cache) inRAM(keys []string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.ram {
		if extends(keys, r) {
			return true
		}
	}
	return false
}

// willPark reports whether the engine parks a conversation of this many
// tokens on the switch away from it. It evicts parked ones to make room, so it
// refuses only one bigger than the whole budget, or one that leaves less than
// the RAM floor even with every parked one evicted.
func (c *cache) willPark(tokens int, en strata.Engine) bool {
	minTokens := c.pol.ParkMinTokens
	if minTokens < 0 {
		minTokens = en.MinTokens
	}
	est := int64(tokens) * c.pol.SnapshotBytesPerToken
	if en.BudgetMiB <= 0 || tokens < minTokens || est > en.BudgetMiB<<20 {
		return false
	}
	avail, err := c.pol.MemAvailable()
	return err == nil && est+en.MinFreeMiB<<20 <= avail+en.ParkedBytes
}

func (c *cache) skip(reason string, tokens int) {
	c.m.skipped.WithLabelValues(reason).Inc()
	c.log.Info("save skipped", "reason", reason, "tokens", tokens)
}

// save writes the live conversation to disk unless it is short, already on
// disk or doesn't fit. It outlives the request's context: Strata finishes a
// save it started anyway, and the index must know about it.
func (c *cache) save(ctx context.Context, fp string, live []string, tokens int, trigger string) {
	switch {
	case tokens < c.pol.MinTokens:
		c.skip("short", tokens)
		return
	case c.files.Covered(fp, live, tokens-c.pol.RegrowTokens):
		c.skip("on_disk", tokens)
		return
	}
	c.mu.Lock()
	bpt := c.bpt
	c.mu.Unlock()
	if bpt <= 0 {
		bpt = 32 << 10 // ~25 KB/token measured; err high
	}
	if !c.files.Reserve(int64(tokens) * bpt) {
		c.skip("no_room", tokens)
		return
	}
	t0 := time.Now()
	n, _, err := c.eng.Save(context.WithoutCancel(ctx), store.FileName(fp, live[len(live)-1], tokens))
	d := time.Since(t0)
	if err != nil {
		c.m.failures.WithLabelValues("save", strata.ErrorKind(err)).Inc()
		c.log.Warn("save failed", "trigger", trigger, "tokens", tokens, "ms", d.Milliseconds(), "reason", strata.ErrorKind(err), "err", err)
		return
	}
	e, err := c.files.Commit(fp, live, tokens, n)
	if err != nil {
		c.m.failures.WithLabelValues("save", "index").Inc()
		c.log.Warn("save not indexed", "err", err)
		return
	}
	c.m.saved(trigger, e.Size, d)
	c.mu.Lock()
	c.bpt = e.Size / int64(e.Tokens)
	c.mu.Unlock()
	c.log.Info("save", "trigger", trigger, "tokens", e.Tokens, "mb", e.Size>>20, "ms", d.Milliseconds())
}

func (c *cache) restore(ctx context.Context, e store.Entry, t *ticket) bool {
	uCtx := context.WithoutCancel(ctx)
	t0 := time.Now()
	n, b, err := c.eng.Restore(uCtx, e.Name())
	if strata.IsUnloaded(err) {
		// A --lazy engine loads on the next request: wake it, check it is still the engine
		// the file was written for, and give it the restore it refused.
		c.log.Info("waking engine for restore", "tokens", e.Tokens)
		if err = c.eng.Wakeup(uCtx); err == nil {
			en, eerr := c.eng.Engine(uCtx)
			if eerr != nil {
				err = eerr
			} else {
				switch fp := c.learn(en); {
				case fp != e.Fingerprint:
					c.log.Warn("woken into another engine; reading fresh", "wanted", e.Fingerprint, "got", fp)
					c.m.failures.WithLabelValues("restore", "fingerprint").Inc()
					return false
				default:
					t0 = time.Now()
					n, b, err = c.eng.Restore(uCtx, e.Name())
				}
			}
		}
	}
	d := time.Since(t0)
	if err != nil {
		kind := strata.ErrorKind(err)
		c.m.failures.WithLabelValues("restore", kind).Inc()
		c.log.Warn("restore failed; reading fresh", "tokens", e.Tokens, "ms", d.Milliseconds(), "reason", kind, "err", err)
		if kind == "not_found" || kind == "invalid" || kind == "io" { // one it had no RAM for stays
			c.files.Forget(e, "unreadable")
		}
		return false
	}
	if n < e.Tokens/2 {
		// The file on disk restored far less than its name claims: a save cut short. Keep it
		// and every later turn finds it "covered" and is never saved.
		c.log.Warn("truncated session file; forgetting", "expected", e.Tokens, "actual", n)
		c.files.Forget(e, "truncated")
	} else {
		c.files.Touch(e)
	}
	c.m.restored(b, d)
	t.tokens = n
	c.log.Info("restore", "tokens", n, "mb", b>>20, "ms", d.Milliseconds())
	return true
}

// finish records the end of t's request: its HTTP status, and whether the
// client went away before the answer was complete.
func (c *cache) finish(t *ticket, status int, clientGone bool) {
	c.m.requests.WithLabelValues(t.action).Inc()
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.counted {
		c.inflight--
	}
	if status >= 500 {
		c.ram = nil // the engine may have died, and its RAM cache with it
	}
	switch {
	case t.action != actContinue && t.action != actRAM && t.action != actDisk && t.action != actMiss,
		c.seq != t.seq, status != 200, clientGone:
		c.live = nil // another request may have run in between, or this one left no clean state
		return
	}
	if t.prevParks {
		c.ram = append([][]string{t.prev}, dropTurnsOf(c.ram, t.prev)...)
	}
	c.ram = dropTurnsOf(c.ram, t.keys) // now live, not parked
	c.live = t.keys
	if c.pol.IdleDelay > 0 && c.ctx.Err() == nil {
		if c.idle != nil && c.idle.Stop() {
			c.wg.Done()
		}
		c.wg.Add(1)
		seq := t.seq
		c.idle = time.AfterFunc(c.pol.IdleDelay, func() { defer c.wg.Done(); c.idleSave(seq) })
	}
}

// idleSave saves the live conversation if nothing has started since the turn
// numbered seq ended.
func (c *cache) idleSave(seq uint64) {
	defer func() {
		if v := recover(); v != nil {
			c.log.Error("cache panic in an idle save", "panic", fmt.Sprint(v))
			c.m.panics.Inc()
		}
	}()
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	c.mu.Lock()
	live, fp := c.live, c.fingerprint
	quiet := c.seq == seq && c.inflight == 0 && live != nil && fp != "" && c.ctx.Err() == nil
	c.mu.Unlock()
	if !quiet {
		return
	}
	if st, err := c.eng.Status(c.ctx); err == nil && !c.restarted(st) {
		c.save(c.ctx, fp, live, st.LiveTokens, "idle")
	}
}

// dropTurnsOf removes the conversations keys extends (earlier turns of it).
func dropTurnsOf(l [][]string, keys []string) [][]string {
	out := l[:0:0]
	for _, r := range l {
		if !extends(keys, r) {
			out = append(out, r)
		}
	}
	return out
}
