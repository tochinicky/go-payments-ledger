// Package obs sets up observability for every binary: OpenTelemetry metrics exposed in Prometheus format, traces
// sent over OTLP when OTEL_EXPORTER_OTLP_ENDPOINT is set, and an admin listener with /metrics, /healthz and /readyz.
package obs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// LatencyBuckets are the HTTP server duration histogram's bucket bounds, in seconds. 0.1 is one of them on purpose:
// the SLO is "99% of transfers under 100 ms", and with a bucket edge at exactly 100 ms that is a count of requests,
// not an interpolation between buckets.
var LatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1}

// Telemetry is one process's metrics registry and providers.
type Telemetry struct {
	Registry *prometheus.Registry
	shutdown []func(context.Context) error
}

// Setup installs the global meter and tracer providers for service. Traces go to OTLP only if an endpoint is
// configured (the standard OTEL_EXPORTER_OTLP_* variables); otherwise tracing is a no-op.
func Setup(ctx context.Context, service string) (*Telemetry, error) {
	// Schemaless, so the merge never conflicts with the SDK's own semconv schema version after an upgrade.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(service)))
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "http.server.request.duration"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: LatencyBuckets}},
		)),
	)
	otel.SetMeterProvider(mp)
	t := &Telemetry{Registry: reg, shutdown: []func(context.Context) error{mp.Shutdown}}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		t.shutdown = append(t.shutdown, tp.Shutdown)
	}
	return t, nil
}

// Shutdown flushes and stops the providers.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, f := range t.shutdown {
		errs = append(errs, f(ctx))
	}
	return errors.Join(errs...)
}

// Health is the admin listener's view of the process: Ready decides /readyz, and Drain makes it fail from now on
// (on shutdown, so the load balancer stops sending traffic before the server stops accepting it).
type Health struct {
	Ready    func(ctx context.Context) error // nil means always ready
	draining atomic.Bool
}

// Drain makes /readyz answer 503 from now on.
func (h *Health) Drain() { h.draining.Store(true) }

// AdminHandler serves /metrics, /healthz (liveness: the process is up and serving HTTP; it must not depend on the
// database or Kafka, or an outage of either would make the orchestrator restart every replica and turn a
// dependency failure into a full outage) and /readyz (readiness: may this process take traffic now).
func (t *Telemetry) AdminHandler(h *Health) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if h.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		if h.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := h.Ready(ctx); err != nil {
				http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
