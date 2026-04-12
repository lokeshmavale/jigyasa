package com.jigyasa.dp.search.metrics;

/**
 * Lifecycle interface for metrics infrastructure (HTTP server, registry).
 * Only the server wrapper calls these — handlers never do.
 */
public interface MetricsLifecycle {

    /** Start the metrics HTTP server. */
    void start();

    /** Stop the metrics HTTP server and release resources. */
    void stop();
}
