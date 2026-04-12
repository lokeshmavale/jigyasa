package com.jigyasa.dp.search.models;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Creates IndexSearcher instances with a shared Executor for concurrent segment search.
 * Uses ES-style thread sizing: (availableProcessors * 3/2) + 1 to keep CPU busy
 * during minor I/O stalls (MMap page faults on DocValues/stored fields).
 */
@RequiredArgsConstructor
public class CustomSearcherFactory extends SearcherFactory {
    private final Similarity similarity;

    private static final int SEARCH_THREADS = (Runtime.getRuntime().availableProcessors() * 3 / 2) + 1;

    private static final Executor SEARCH_EXECUTOR = Executors.newFixedThreadPool(
            SEARCH_THREADS,
            r -> {
                Thread t = new Thread(r, "jigyasa-search");
                t.setDaemon(true);
                return t;
            });

    /** Shared executor for concurrent segment search. Used by PerRequestSearcher. */
    public static Executor getSearchExecutor() {
        return SEARCH_EXECUTOR;
    }

    @Override
    public IndexSearcher newSearcher(IndexReader reader, IndexReader prevReader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader, SEARCH_EXECUTOR);
        searcher.setSimilarity(similarity);
        return searcher;
    }
}
