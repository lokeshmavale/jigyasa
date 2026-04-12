package com.jigyasa.dp.search.metrics;

/** Zero-overhead implementation when metrics are disabled. All methods are no-ops. */
public final class NoopMetricsService implements MetricsService {
    @Override public void recordRequest(String rpc, String collection, String status, long d) {}
    @Override public void incrementActiveRequests(String rpc) {}
    @Override public void decrementActiveRequests(String rpc) {}
    @Override public void recordFacet(String collection, String facetType, long d) {}
    @Override public void recordIndexDuration(String collection, long d) {}
    @Override public void recordIndexedDocs(String collection, long count) {}
    @Override public void start() {}
    @Override public void stop() {}
}
