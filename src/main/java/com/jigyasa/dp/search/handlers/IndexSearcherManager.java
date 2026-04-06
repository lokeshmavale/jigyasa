package com.jigyasa.dp.search.handlers;

import org.apache.lucene.search.IndexSearcher;

/**
 * Abstraction over IndexSearcher lifecycle: acquire/release with try-with-resources support.
 */
public interface IndexSearcherManager {

    record SearcherLease(IndexSearcher searcher, IndexSearcherManager mgr) implements AutoCloseable {
        @Override public void close() { mgr.releaseSearcher(searcher); }
    }

    IndexSearcher acquireSearcher();
    void releaseSearcher(IndexSearcher searcher);
    SearcherLease leaseSearcher();
    void waitForGeneration(long generation);
    void forceRefresh();
    void shutdown();
}
