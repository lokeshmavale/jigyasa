package com.jigyasa.dp.search.handlers;

import org.apache.lucene.index.IndexWriter;

/**
 * Abstraction over IndexWriter lifecycle: acquire/release with try-with-resources support.
 */
public interface IndexWriterManager {

    record WriterLease(IndexWriter writer, IndexWriterManager mgr) implements AutoCloseable {
        @Override public void close() { mgr.releaseWriter(); }
    }

    IndexWriter acquireWriter();
    void releaseWriter();
    WriterLease leaseWriter();
    IndexWriter getWriter();
    void shutdown();
}
