package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.CustomSearcherFactory;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.ServerMode;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

@RequiredArgsConstructor
public class IndexSearcherManagerISCH implements IndexSchemaChangeHandler {
    private static final Logger log = LoggerFactory.getLogger(IndexSearcherManagerISCH.class);

    // NRT refresh tuning: min 25ms between refreshes, max 1s staleness when waiters exist
    private static final double TARGET_MIN_STALE_SEC = 0.025;
    private static final double TARGET_MAX_STALE_SEC = 1.0;
    private static final int DEFAULT_WAIT_FOR_GEN_MS = 5000;

    private final ServerMode mode;
    private final IndexWriterManagerISCH writerManagerISCH;
    private final String indexCacheDir;
    private volatile SearcherManager searcherManager;
    private volatile Directory ownedDirectory;
    private volatile ControlledRealTimeReopenThread<IndexSearcher> nrtReopenThread;
    private final Object searcherLock = new Object();

    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        synchronized (this.searcherLock) {
            if (mode == ServerMode.WRITE || mode == ServerMode.READ_WRITE) {
                try {
                    shutdownNrtThread();
                    closeCurrentSearcherManager();
                    this.searcherManager = new SearcherManager(
                            writerManagerISCH.acquireWriter(),
                            new CustomSearcherFactory(newIndexSchema.getInitializedSchema().getBm25Similarity()));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize searcher manager", e);
                } finally {
                    writerManagerISCH.releaseWriter();
                }
            } else {
                try {
                    if (searcherManager != null) {
                        return;
                    }
                    Directory directory = FSDirectory.open(Path.of(indexCacheDir));
                    try {
                        this.searcherManager = new SearcherManager(
                                directory,
                                new CustomSearcherFactory(newIndexSchema.getInitializedSchema().getBm25Similarity()));
                        this.ownedDirectory = directory;
                    } catch (Exception e) {
                        directory.close();
                        throw e;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize searcher manager", e);
                }
            }
        }
    }

    public IndexSearcher acquireSearcher() {
        try {
            return this.searcherManager.acquire();
        } catch (IOException e) {
            throw new RuntimeException("Failed to acquire searcher", e);
        }
    }

    public record SearcherLease(IndexSearcher searcher, IndexSearcherManagerISCH mgr) implements AutoCloseable {
        @Override public void close() { mgr.releaseSearcher(searcher); }
    }

    public SearcherLease leaseSearcher() {
        return new SearcherLease(acquireSearcher(), this);
    }

    public void releaseSearcher(IndexSearcher searcher) {
        try {
            this.searcherManager.release(searcher);
        } catch (IOException e) {
            log.warn("Error releasing searcher", e);
        }
    }

    /**
     * Blocks until the given IndexWriter generation is visible to searchers.
     * Use after indexing to guarantee the just-written documents are searchable.
     *
     * @param generation the generation token returned by IndexWriter.updateDocument/addDocument
     */
    public void waitForGeneration(long generation) {
        ControlledRealTimeReopenThread<IndexSearcher> thread = this.nrtReopenThread;
        if (thread == null) {
            log.warn("NRT reopen thread not started; falling back to manual refresh");
            try {
                searcherManager.maybeRefreshBlocking();
            } catch (IOException e) {
                throw new RuntimeException("Fallback NRT refresh failed", e);
            }
            return;
        }
        try {
            thread.waitForGeneration(generation, DEFAULT_WAIT_FOR_GEN_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for generation " + generation, e);
        }
    }

    public void forceRefresh() {
        try {
            searcherManager.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new RuntimeException("Force refresh failed", e);
        }
    }

    private void closeCurrentSearcherManager() {
        if (searcherManager != null) {
            try {
                searcherManager.close();
                log.info("Previous SearcherManager closed");
            } catch (IOException e) {
                log.warn("Error closing previous SearcherManager", e);
            }
        }
    }

    private void shutdownNrtThread() {
        if (nrtReopenThread != null) {
            try {
                nrtReopenThread.close();
                nrtReopenThread.join(2000);
                log.info("NRT reopen thread stopped");
            } catch (Exception e) {
                log.warn("Error stopping NRT reopen thread", e);
            }
            nrtReopenThread = null;
        }
    }

    @Override
    public void initService() {
        if (mode == ServerMode.WRITE || mode == ServerMode.READ_WRITE) {
            // Start NRT reopen thread — it uses IndexWriter's generation tracking internally
            ControlledRealTimeReopenThread<IndexSearcher> thread =
                    new ControlledRealTimeReopenThread<>(
                            writerManagerISCH.getWriter(),
                            searcherManager,
                            TARGET_MAX_STALE_SEC,
                            TARGET_MIN_STALE_SEC);
            thread.setName("jigyasa-nrt-reopen");
            thread.setDaemon(true);
            thread.start();
            this.nrtReopenThread = thread;
            log.info("NRT reopen thread started (minStale={}ms, maxStale={}ms)",
                    (long)(TARGET_MIN_STALE_SEC * 1000), (long)(TARGET_MAX_STALE_SEC * 1000));
        } else {
            // READ-only mode: periodic refresh from directory (no writer available)
            log.info("READ mode: NRT not applicable, searcher refreshes on schema change only");
        }
    }

    public void shutdown() {
        shutdownNrtThread();
        try {
            if (searcherManager != null) {
                searcherManager.close();
            }
        } catch (IOException e) {
            log.warn("Error closing SearcherManager", e);
        }
        try {
            if (ownedDirectory != null) {
                ownedDirectory.close();
            }
        } catch (IOException e) {
            log.warn("Error closing Directory", e);
        }
    }
}
