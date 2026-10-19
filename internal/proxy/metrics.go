package proxy

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tfyl/strata-diskcache/internal/store"
)

// Metrics are the proxy's Prometheus metrics, all strata_diskcache_*.
type Metrics struct {
	requests, savesBy, skipped, failures, evictions, evictedBytes *prometheus.CounterVec
	saves, restores, savedBytes, restoredBytes, panics            prometheus.Counter
	saveSeconds, restoreSeconds                                   prometheus.Histogram
	lastSave, lastRestore                                         prometheus.Gauge
}

// NewMetrics registers the metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: "strata_diskcache", Name: name, Help: help}
	}
	vec := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts(opts(name, help)), labels)
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts(opts(name, help)))
	}
	hist := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "strata_diskcache", Name: name, Help: help,
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}})
	}
	m := &Metrics{
		requests:       vec("requests_total", "Generation requests by what the cache did.", "action"),
		savesBy:        vec("saves_by_trigger_total", "Session files saved, by trigger.", "trigger"),
		skipped:        vec("saves_skipped_total", "Saves not made, by reason.", "reason"),
		failures:       vec("failures_total", "Saves and restores Strata refused or failed.", "op", "reason"),
		evictions:      vec("evictions_total", "Session files deleted, by reason.", "reason"),
		evictedBytes:   vec("evicted_bytes_total", "Bytes of session files deleted, by reason.", "reason"),
		saves:          counter("saves_total", "Session files saved."),
		restores:       counter("restores_total", "Session files restored."),
		savedBytes:     counter("saved_bytes_total", "Bytes written to session files."),
		restoredBytes:  counter("restored_bytes_total", "Bytes read from session files."),
		panics:         counter("panics_total", "Panics in the cache logic (the request went through)."),
		saveSeconds:    hist("save_seconds", "Time to save a session file."),
		restoreSeconds: hist("restore_seconds", "Time to restore a session file."),
		lastSave:       prometheus.NewGauge(prometheus.GaugeOpts(opts("last_save_ms", "Duration of the last save."))),
		lastRestore:    prometheus.NewGauge(prometheus.GaugeOpts(opts("last_restore_ms", "Duration of the last restore."))),
	}
	reg.MustRegister(m.requests, m.savesBy, m.skipped, m.failures, m.evictions, m.evictedBytes, m.saves, m.restores,
		m.savedBytes, m.restoredBytes, m.panics, m.saveSeconds, m.restoreSeconds, m.lastSave, m.lastRestore)
	return m
}

// WatchStore exports the store's file count, total size and cap.
func (m *Metrics) WatchStore(reg prometheus.Registerer, st *store.Store, maxBytes int64) {
	gauge := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "strata_diskcache", Name: name, Help: help}, f)
	}
	reg.MustRegister(
		gauge("files", "Session files on disk.", func() float64 { n, _ := st.Stats(); return float64(n) }),
		gauge("bytes", "Total size of the session files.", func() float64 { _, b := st.Stats(); return float64(b) }),
		gauge("max_bytes", "The session files' size cap.", func() float64 { return float64(maxBytes) }),
	)
}

// Evicted counts a deleted file (the store's OnEvict).
func (m *Metrics) Evicted(reason string, bytes int64) {
	m.evictions.WithLabelValues(reason).Inc()
	m.evictedBytes.WithLabelValues(reason).Add(float64(bytes))
}

func (m *Metrics) saved(trigger string, bytes int64, d time.Duration) {
	m.saves.Inc()
	m.savesBy.WithLabelValues(trigger).Inc()
	m.savedBytes.Add(float64(bytes))
	m.saveSeconds.Observe(d.Seconds())
	m.lastSave.Set(float64(d.Milliseconds()))
}

func (m *Metrics) restored(bytes int64, d time.Duration) {
	m.restores.Inc()
	m.restoredBytes.Add(float64(bytes))
	m.restoreSeconds.Observe(d.Seconds())
	m.lastRestore.Set(float64(d.Milliseconds()))
}
