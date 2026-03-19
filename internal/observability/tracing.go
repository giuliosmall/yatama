// Package observability provides OpenTelemetry tracing and extended Prometheus
// metrics for the task-manager system. Traces flow end-to-end from HTTP
// request through Kafka produce/consume to task execution by propagating
// trace context in Kafka record headers.
package observability

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TracerName is the instrumentation library name used by all spans.
const TracerName = "github.com/giulio/task-manager"

// InitTracer sets up the OpenTelemetry TracerProvider with an OTLP HTTP
// exporter. The returned shutdown function flushes spans and should be called
// on process exit.
//
// If endpoint is empty, tracing is disabled and a no-op provider is used.
func InitTracer(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		slog.Info("tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("init tracer: create exporter: %w", err)
	}

	// Sample at 1/1000 in production to control costs.
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.001))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	slog.Info("tracing initialized", "endpoint", endpoint, "service", serviceName)
	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global provider.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// ---------------------------------------------------------------------------
// Kafka header trace context propagation
// ---------------------------------------------------------------------------

// kafkaHeaderCarrier adapts Kafka record headers for OpenTelemetry propagation.
type kafkaHeaderCarrier struct {
	headers *[]kgo.RecordHeader
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	// Replace existing header if present.
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectTraceContext writes the current span context into Kafka record headers.
// Call this on the producer side before producing a record.
func InjectTraceContext(ctx context.Context, headers *[]kgo.RecordHeader) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: headers})
}

// ExtractTraceContext reads span context from Kafka record headers and returns
// a new context with the extracted span. Call this on the consumer side.
func ExtractTraceContext(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})
}
