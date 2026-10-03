// Package obs provides structured logging in Cloud Logging's JSON format and
// a small dependency-free Prometheus metrics registry.
package obs

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// NewLogger returns a JSON logger whose field names Cloud Logging
// understands (severity, message, time). format "text" gives human output.
func NewLogger(w io.Writer, level, format string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if strings.ToLower(format) == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) > 0 {
			return a
		}
		switch a.Key {
		case slog.LevelKey:
			sev := a.Value.String()
			if sev == "WARN" {
				sev = "WARNING"
			}
			return slog.String("severity", sev)
		case slog.MessageKey:
			return slog.Attr{Key: "message", Value: a.Value}
		}
		return a
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// DefaultLogger builds a logger from STRATA_LOG_LEVEL / STRATA_LOG_FORMAT.
func DefaultLogger() *slog.Logger {
	return NewLogger(os.Stderr, os.Getenv("STRATA_LOG_LEVEL"), os.Getenv("STRATA_LOG_FORMAT"))
}

// ---------------------------------------------------------------- metrics

type collector interface {
	write(w io.Writer)
	metricName() string
}

// Registry holds metrics and renders the Prometheus text exposition format.
type Registry struct {
	mu         sync.Mutex
	collectors []collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

// Write renders all metrics.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	cs := append([]collector(nil), r.collectors...)
	r.mu.Unlock()
	sort.Slice(cs, func(i, j int) bool { return cs[i].metricName() < cs[j].metricName() })
	for _, c := range cs {
		c.write(w)
	}
}

// Handler serves the registry at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

func labelString(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		v := ""
		if i < len(values) {
			v = values[i]
		}
		parts[i] = fmt.Sprintf(`%s="%s"`, n, strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// CounterVec is a counter partitioned by labels.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	values     map[string]float64
	keys       map[string][]string
}

// NewCounterVec registers a counter.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, values: map[string]float64{}, keys: map[string][]string{}}
	r.register(c)
	return c
}

func (c *CounterVec) metricName() string { return c.name }

// Add increments the counter for the label values.
func (c *CounterVec) Add(v float64, labelValues ...string) {
	k := strings.Join(labelValues, "\x00")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[k] += v
	c.keys[k] = labelValues
}

// Inc adds one.
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Value returns the current value (tests).
func (c *CounterVec) Value(labelValues ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[strings.Join(labelValues, "\x00")]
}

func (c *CounterVec) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %s\n", c.name, labelString(c.labels, c.keys[k]), formatFloat(c.values[k]))
	}
}

// HistogramVec is a histogram partitioned by labels.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*histSeries
}

type histSeries struct {
	labels []string
	counts []uint64
	sum    float64
	count  uint64
}

// DefaultBuckets suit request latencies in seconds.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// LongBuckets suit cloud API calls and operations in seconds.
var LongBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}

// NewHistogramVec registers a histogram.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*histSeries{}}
	r.register(h)
	return h
}

func (h *HistogramVec) metricName() string { return h.name }

// Observe records a value.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	k := strings.Join(labelValues, "\x00")
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &histSeries{labels: labelValues, counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, b := range h.buckets {
		if v <= b {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.series))
	for k := range h.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := h.series[k]
		for i, b := range h.buckets {
			lbl := labelString(append(append([]string(nil), h.labels...), "le"), append(append([]string(nil), s.labels...), formatFloat(b)))
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, lbl, s.counts[i])
		}
		lbl := labelString(append(append([]string(nil), h.labels...), "le"), append(append([]string(nil), s.labels...), "+Inf"))
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, lbl, s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelString(h.labels, s.labels), formatFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelString(h.labels, s.labels), s.count)
	}
}

// GaugeFunc reports a value computed at scrape time.
type GaugeFunc struct {
	name, help string
	fn         func() float64
}

// NewGaugeFunc registers a gauge.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) *GaugeFunc {
	g := &GaugeFunc{name: name, help: help, fn: fn}
	r.register(g)
	return g
}

func (g *GaugeFunc) metricName() string { return g.name }

func (g *GaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", g.name, g.help, g.name, g.name, formatFloat(g.fn()))
}

// Metrics is the set of metrics Strata exports.
type Metrics struct {
	Registry          *Registry
	HTTPRequests      *CounterVec
	HTTPDuration      *HistogramVec
	ProviderCalls     *CounterVec
	ProviderDuration  *HistogramVec
	Operations        *CounterVec
	OperationDuration *HistogramVec
}

// NewMetrics creates and registers Strata's metrics.
func NewMetrics() *Metrics {
	r := NewRegistry()
	return &Metrics{
		Registry:          r,
		HTTPRequests:      r.NewCounterVec("strata_http_requests_total", "HTTP requests by route and status code.", "method", "route", "code"),
		HTTPDuration:      r.NewHistogramVec("strata_http_request_duration_seconds", "HTTP request latency.", DefaultBuckets, "method", "route"),
		ProviderCalls:     r.NewCounterVec("strata_provider_calls_total", "Cloud provider calls by type, action and result.", "type", "action", "result"),
		ProviderDuration:  r.NewHistogramVec("strata_provider_call_duration_seconds", "Cloud provider call latency including LRO polling.", LongBuckets, "type", "action"),
		Operations:        r.NewCounterVec("strata_operations_total", "Finished stack operations by kind and result.", "kind", "result"),
		OperationDuration: r.NewHistogramVec("strata_operation_duration_seconds", "Stack operation duration.", LongBuckets, "kind"),
	}
}
