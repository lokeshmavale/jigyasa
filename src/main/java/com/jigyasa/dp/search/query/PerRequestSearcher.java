package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import com.jigyasa.dp.search.models.CustomSearcherFactory;
import org.apache.lucene.search.IndexSearcher;

import java.util.concurrent.TimeUnit;

/**
 * Per-request IndexSearcher wrapper that provides query timeout isolation.
 *
 * <p>Problem: {@link IndexSearcher#setTimeout(QueryTimeout)} mutates a shared instance field.
 * With concurrent queries on the same searcher (from SearcherManager), timeouts race.
 *
 * <p>Solution (same as Elasticsearch's ContextIndexSearcher): create a lightweight per-request
 * IndexSearcher wrapping the shared IndexReader. The per-request instance owns its own
 * {@code queryTimeout} and {@code partialResult} fields — no races.
 *
 * <p>Cost: ~10µs per request (constructor + lazy LeafSlice computation). The expensive
 * IndexReader, QueryCache, and Executor are all shared.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/blob/8.13/server/src/main/java/org/elasticsearch/search/internal/ContextIndexSearcher.java">
 *     ES ContextIndexSearcher</a>
 */
public class PerRequestSearcher implements AutoCloseable {

    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    private final IndexSearcher searcher;
    private final IndexSearcher original;

    /**
     * Creates a per-request searcher with optional timeout.
     *
     * @param lease      the searcher lease from SearcherManager (holds incRef)
     * @param timeoutMs  timeout in milliseconds. 0 = server default (30s). Negative = no timeout.
     */
    public PerRequestSearcher(IndexSearcherManager.SearcherLease lease, int timeoutMs) {
        this.original = lease.searcher();

        // Lightweight wrapper: shares IndexReader + Executor, owns timeout state
        this.searcher = new IndexSearcher(
                original.getIndexReader(),
                CustomSearcherFactory.getSearchExecutor()
        );
        searcher.setSimilarity(original.getSimilarity());
        searcher.setQueryCache(original.getQueryCache());
        searcher.setQueryCachingPolicy(original.getQueryCachingPolicy());

        int effectiveTimeout = resolveTimeout(timeoutMs);
        if (effectiveTimeout > 0) {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveTimeout);
            searcher.setTimeout(() -> System.nanoTime() > deadlineNanos);
        }
    }

    /** The per-request searcher with isolated timeout. Use this for all search operations. */
    public IndexSearcher searcher() {
        return searcher;
    }

    /** True if the query hit the timeout. Check after search completes. */
    public boolean timedOut() {
        return searcher.timedOut();
    }

    /** Resolves effective timeout: 0 → server default, negative → no timeout. */
    private static int resolveTimeout(int requestedMs) {
        if (requestedMs == 0) return DEFAULT_TIMEOUT_MS;
        if (requestedMs < 0) return -1;
        return requestedMs;
    }

    @Override
    public void close() {
        // Lease is closed by the caller (try-with-resources in handler).
        // We don't own the IndexReader lifecycle — the original searcher does.
        // Nothing to clean up on the per-request wrapper.
    }
}
