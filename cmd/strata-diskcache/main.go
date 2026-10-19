// Command strata-diskcache is a reverse proxy that gives Strata
// (github.com/Niko1221/Strata) an on-disk conversation cache.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/tfyl/strata-diskcache/internal/proxy"
	"github.com/tfyl/strata-diskcache/internal/store"
)

// version is set at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

// config is the command line. Every flag can also be set by its environment
// variable; the flag wins.
type config struct {
	Listen        string `env:"LISTEN" default:":8080" help:"Address clients connect to."`
	Upstream      string `env:"UPSTREAM" default:"http://127.0.0.1:8081" help:"Strata's URL."`
	MetricsListen string `env:"METRICS_LISTEN" default:"127.0.0.1:8082" help:"Address for /metrics, /requests and /health. Keep it private."`
	SessionDir    string `env:"SESSION_DIR" help:"Strata's slot_save_path. Empty: a plain proxy, no disk cache."`

	MaxGiB        float64 `name:"max-gib" env:"MAX_GIB" default:"40" help:"Total size cap of the session files, GiB. Least recently used go first."`
	RetentionDays float64 `env:"RETENTION_DAYS" default:"7" help:"Delete session files unused for this many days."`
	MinFreeMiB    float64 `name:"min-free-mib" env:"MIN_FREE_MIB" default:"4096" help:"Never leave less than this free on the session filesystem. Match Strata's --session-min-free-mib."`

	MinTokens             int     `env:"MIN_TOKENS" default:"15000" help:"Save conversations of at least this many tokens."`
	ParkMinTokens         int     `env:"PARK_MIN_TOKENS" default:"-1" help:"Strata's --conversation-cache-min-tokens. -1 reads it from Strata."`
	RegrowTokens          int     `env:"REGROW_TOKENS" default:"4096" help:"Save a conversation again once it has grown this much past its file."`
	IdleSaveS             float64 `name:"idle-save-s" env:"IDLE_SAVE_S" default:"3" help:"Quiet seconds after a turn before it is saved. 0: save only when a switch would lose it."`
	SnapshotBytesPerToken int64   `env:"SNAPSHOT_BYTES_PER_TOKEN" default:"65536" help:"RAM per token of a parked conversation, to predict when Strata will refuse to park one."`
	MaxBodyMiB            int64   `name:"max-body-mib" env:"MAX_BODY_MIB" default:"64" help:"Generation request bodies larger than this are streamed through without caching."`
	ShutdownS             float64 `name:"shutdown-s" env:"SHUTDOWN_S" default:"20" help:"On SIGTERM, how long running requests and a save under way get to finish."`

	Log struct {
		Output     string `env:"OUTPUT" enum:"stderr,file,both" default:"stderr" help:"Where logs go: stderr, file or both."`
		File       string `env:"FILE" default:"/var/log/strata-diskcache/strata-diskcache.log" help:"Log file, for file and both."`
		Format     string `env:"FORMAT" enum:"text,json" default:"text" help:"text or json."`
		Level      string `env:"LEVEL" enum:"debug,info,warn,error" default:"info" help:"debug, info, warn or error."`
		MaxSizeMiB int    `name:"max-size-mib" env:"MAX_SIZE_MIB" default:"100" help:"Rotate the log file at this size."`
		MaxBackups int    `env:"MAX_BACKUPS" default:"10" help:"Rotated log files to keep (0: all)."`
		MaxAgeDays int    `env:"MAX_AGE_DAYS" default:"30" help:"Delete rotated log files older than this (0: never)."`
		Compress   bool   `env:"COMPRESS" default:"true" negatable:"" help:"gzip rotated log files."`
	} `embed:"" prefix:"log-" envprefix:"LOG_" group:"Logging"`

	Version kong.VersionFlag `help:"Print the version and exit."`
}

// Validate is called by kong after parsing.
func (c *config) Validate() error {
	if u, err := url.Parse(c.Upstream); err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("--upstream must be a URL like http://127.0.0.1:8081")
	}
	if c.MaxGiB <= 0 || c.RetentionDays <= 0 || c.MinTokens < 1 || c.SnapshotBytesPerToken <= 0 || c.MaxBodyMiB < 1 ||
		c.ShutdownS <= 0 || c.MinFreeMiB < 0 || c.IdleSaveS < 0 || c.RegrowTokens < 0 || c.Log.MaxSizeMiB < 1 {
		return errors.New("sizes, limits and timeouts must be positive (--min-free-mib, --idle-save-s and --regrow-tokens may be 0)")
	}
	return nil
}

func parse(args []string) (*config, error) {
	var c config
	k, err := kong.New(&c, kong.Name("strata-diskcache"),
		kong.Description("A reverse proxy that gives Strata an on-disk conversation cache."),
		kong.Vars{"version": "strata-diskcache " + version}, kong.UsageOnError())
	if err != nil {
		return nil, err
	}
	_, err = k.Parse(args)
	return &c, err
}

// newLogger builds the slog logger and a closer for its file. A log file is
// appended to across restarts and rotates by size into
// <name>-<timestamp>.log(.gz) next to it.
func newLogger(c *config, stderr io.Writer) (*slog.Logger, io.Closer, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Log.Level)); err != nil {
		return nil, nil, err
	}
	w, closer := stderr, io.Closer(io.NopCloser(nil))
	if c.Log.Output != "stderr" {
		if err := os.MkdirAll(filepath.Dir(c.Log.File), 0o755); err != nil {
			return nil, nil, fmt.Errorf("log folder: %w", err)
		}
		f := &lumberjack.Logger{Filename: c.Log.File, MaxSize: c.Log.MaxSizeMiB, MaxBackups: c.Log.MaxBackups,
			MaxAge: c.Log.MaxAgeDays, Compress: c.Log.Compress}
		w, closer = f, f
		if c.Log.Output == "both" {
			w = io.MultiWriter(stderr, f)
		}
	}
	opts := &slog.HandlerOptions{Level: level}
	if c.Log.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts)), closer, nil
	}
	return slog.New(slog.NewTextHandler(w, opts)), closer, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "strata-diskcache:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parse(os.Args[1:])
	if err != nil {
		return err
	}
	log, closeLog, err := newLogger(cfg, os.Stderr)
	if err != nil {
		return err
	}
	defer func() { _ = closeLog.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var lc net.ListenConfig
	pl, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return err
	}
	al, err := lc.Listen(ctx, "tcp", cfg.MetricsListen)
	if err != nil {
		return err
	}
	return serve(ctx, cfg, log.With("version", version), pl, al)
}

// serve runs the proxy and the admin server on the listeners until ctx ends,
// then gives running requests and a save under way --shutdown-s to finish.
func serve(ctx context.Context, cfg *config, log *slog.Logger, proxyL, adminL net.Listener) error {
	upstream, _ := url.Parse(cfg.Upstream)
	maxBytes := int64(cfg.MaxGiB * (1 << 30))
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := proxy.NewMetrics(reg)

	var st *store.Store
	if cfg.SessionDir != "" {
		st = store.New(store.Options{Dir: cfg.SessionDir, MaxBytes: maxBytes, Log: log, OnEvict: m.Evicted,
			MaxAge: time.Duration(cfg.RetentionDays * 24 * float64(time.Hour)), MinFree: int64(cfg.MinFreeMiB * (1 << 20))})
		if err := st.Load(); err != nil {
			log.Warn("session folder unreadable; disk cache off, plain proxy", "dir", cfg.SessionDir, "err", err)
			st = nil
		} else {
			m.WatchStore(reg, st, maxBytes)
			n, _ := st.Stats()
			log.Info("disk cache", "dir", cfg.SessionDir, "max_gib", cfg.MaxGiB, "retention_days", cfg.RetentionDays, "files", n)
		}
	}
	p := proxy.New(proxy.Options{Upstream: upstream, Store: st, MaxBody: cfg.MaxBodyMiB << 20, Metrics: m, Log: log,
		Policy: proxy.Policy{MinTokens: cfg.MinTokens, ParkMinTokens: cfg.ParkMinTokens, RegrowTokens: cfg.RegrowTokens,
			IdleDelay: time.Duration(cfg.IdleSaveS * float64(time.Second)), SnapshotBytesPerToken: cfg.SnapshotBytesPerToken}})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /requests", p.Requests)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	srv := &http.Server{Handler: p, ReadHeaderTimeout: 30 * time.Second}
	admin := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	for _, s := range []struct {
		srv *http.Server
		l   net.Listener
	}{{srv, proxyL}, {admin, adminL}} {
		g.Go(func() error {
			if err := s.srv.Serve(s.l); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
	}
	g.Go(func() error {
		tick := time.NewTicker(10 * time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-gctx.Done():
				log.Info("shutting down")
				sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(cfg.ShutdownS*float64(time.Second)))
				defer cancel()
				return errors.Join(srv.Shutdown(sctx), admin.Shutdown(sctx), p.Close(sctx))
			case <-tick.C:
				if st != nil {
					st.Prune()
				}
			}
		}
	})
	log.Info("listening", "listen", proxyL.Addr().String(), "upstream", cfg.Upstream, "admin", adminL.Addr().String())
	return g.Wait()
}
