package com.jigyasa.dp.search.metrics;

/**
 * Abstraction for recording operational metrics. Implementations must be thread-safe.
 *
 * <p>No vendor types (Micrometer, Prometheus) leak through this interface.
 * Callers import only this — enabling swap to OpenTelemetry or Datadog
 * without changing instrumentation sites.
 *
 * <p>Lifecycle methods ({@link #start()}, {@link #stop()}) are separate from recording
 * to satisfy ISP — callers that only record metrics don't depend on lifecycle.
 */
public interface MetricsService extends MetricsRecorder, MetricsLifecycle {
}
