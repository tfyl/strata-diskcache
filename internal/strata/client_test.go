package strata

import (
	"context"
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
