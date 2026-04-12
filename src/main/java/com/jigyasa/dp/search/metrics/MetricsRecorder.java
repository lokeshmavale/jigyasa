package com.jigyasa.dp.search.metrics;

/**
 * Pure recording interface — no lifecycle, no vendor types.
 * Handlers depend on this; they never call start()/stop().
 */
public interface MetricsRecorder {

    /** Record a completed request with its duration and outcome. */
    void recordRequest(String rpc, String collection, String status, long durationMs);

    /** Increment active request gauge on entry. */
    void incrementActiveRequests(String rpc);

    /** Decrement active request gauge on exit. */
    void decrementActiveRequests(String rpc);

    /** Record facet computation duration by type. */
    void recordFacet(String collection, String facetType, long durationMs);

    /** Record indexing duration for a batch. */
    void recordIndexDuration(String collection, long durationMs);

    /** Increment indexed document counter. */
    void recordIndexedDocs(String collection, long count);
}
