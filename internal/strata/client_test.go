package strata

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/tfyl/strata-diskcache/internal/strata/stratatest"
)

func TestStatusEngineAndFingerprint(t *testing.T) {
	f := stratatest.New(t.TempDir())
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()
	f.Set(16000, 2)

	st, err := c.Status(ctx)
	if err != nil || st != (Status{Started: 1000, Loaded: true, LiveTokens: 16000}) {
		t.Fatalf("status %+v %v", st, err)
	}
	e, err := c.Engine(ctx)
	if err != nil || e.Parked != 2 || e.BudgetMiB != 3584 || e.MinTokens != 15000 || len(e.Fingerprint) != 12 {
		t.Fatalf("engine %+v %v", e, err)
	}
	f.Update(func(f *stratatest.Fake) { f.Model = "other-model" })
	if e2, _ := c.Engine(ctx); e2.Fingerprint == e.Fingerprint {
		t.Error("another model has the same fingerprint")
	}
}

func TestEngineUnloaded(t *testing.T) {
	f := stratatest.New(t.TempDir())
	f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded = true, false })
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := New(srv.URL)
	e, err := c.Engine(context.Background())
	if err == nil || e.Fingerprint != "" {
		t.Errorf("unloaded engine: fingerprint %q, err %v", e.Fingerprint, err)
	}
	if st, err := c.Status(context.Background()); err != nil || st.Loaded {
		t.Errorf("status: %+v %v", st, err)
	}
}

func TestIsUnloaded(t *testing.T) {
	f := stratatest.New(t.TempDir())
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()

	f.Update(func(f *stratatest.Fake) { f.SlotStatus, f.SlotMessage = 503, "engine is not loaded" })
	if _, _, err := c.Restore(ctx, "x.session"); !IsUnloaded(err) {
		t.Errorf("refused by an unloaded engine: %v", err)
	}
	f.Update(func(f *stratatest.Fake) { f.SlotStatus, f.SlotMessage = 503, "refused" })
	if _, _, err := c.Restore(ctx, "x.session"); IsUnloaded(err) {
		t.Errorf("a plain 503 read as unloaded: %v", err)
	}
	f.Update(func(f *stratatest.Fake) { f.SlotStatus, f.SlotMessage = 507, "engine is not loaded" })
	if _, _, err := c.Restore(ctx, "x.session"); IsUnloaded(err) {
		t.Errorf("a 507 read as unloaded: %v", err)
	}
	if IsUnloaded(errors.New("something else")) {
		t.Error("an ordinary error read as unloaded")
	}
}

func TestWakeup(t *testing.T) {
	f := stratatest.New(t.TempDir())
	f.Update(func(f *stratatest.Fake) { f.Unloaded, f.Loaded = true, false })
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()

	if err := c.Wakeup(ctx); err != nil {
		t.Errorf("wakeup: %v", err)
	}
	if acts := f.Take(); len(acts) != 1 || acts[0] != "gen /v1/chat/completions" {
		t.Errorf("actions %q: wakeup must be one generation request", acts)
	}
	if e, err := c.Engine(ctx); err != nil || e.Fingerprint == "" {
		t.Errorf("after a wakeup: %+v %v", e, err)
	}
	f.Update(func(f *stratatest.Fake) { f.Down = true })
	if err := c.Wakeup(ctx); err == nil || ErrorKind(err) != "http_503" {
		t.Errorf("wakeup on a server that is down: %v", err)
	}
}

func TestSlotErrors(t *testing.T) {
	f := stratatest.New(t.TempDir())
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()

	if _, _, err := c.Restore(ctx, "missing.session"); ErrorKind(err) != "not_found" {
		t.Errorf("restore of a missing file: %v", err)
	}
	f.Update(func(f *stratatest.Fake) { f.SlotStatus, f.SlotKind = 507, "storage" })
	if _, _, err := c.Save(ctx, "x.session"); ErrorKind(err) != "storage" {
		t.Errorf("storage refusal: %v", err)
	}
	f.Update(func(f *stratatest.Fake) { f.SlotKind = "" })
	if _, _, err := c.Save(ctx, "x.session"); ErrorKind(err) != "http_507" {
		t.Errorf("refusal without a kind: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.Save(canceled, "x.session"); ErrorKind(err) != "canceled" {
		t.Errorf("canceled: %v", err)
	}
	f.Update(func(f *stratatest.Fake) { f.SlotStatus = 0; f.Tokens = 123 })
	if n, b, err := c.Save(ctx, "y.session"); err != nil || n != 123 || b != 1000 {
		t.Errorf("save: %d %d %v", n, b, err)
	}
}

func TestStrataDown(t *testing.T) {
	c := New("http://127.0.0.1:1")
	if _, err := c.Status(context.Background()); err == nil || ErrorKind(err) != "transport" {
		t.Errorf("status of a server that is down: %v", err)
	}
}

func TestParseMemAvailable(t *testing.T) {
	n, err := parseMemAvailable("MemTotal:       61341696 kB\nMemAvailable:    4561116 kB\n")
	if err != nil || n != 4561116<<10 {
		t.Errorf("got %d %v", n, err)
	}
	if _, err := parseMemAvailable("MemTotal: 1 kB\n"); err == nil {
		t.Error("no MemAvailable accepted")
	}
}
