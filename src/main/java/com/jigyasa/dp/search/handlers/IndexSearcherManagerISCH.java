package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.CustomSearcherFactory;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.ServerMode;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class IndexSearcherManagerISCH implements IndexSchemaChangeHandler {
    private final ServerMode mode;
    private final IndexWriterManagerISCH writerManagerISCH;
    private final String indexCacheDir;
    private volatile SearcherManager searcherManager;
    private final Object searcherLock = new Object();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        synchronized (this.searcherLock) {
            if (mode == ServerMode.WRITE || mode == ServerMode.READ_WRITE) {
                try {
                    this.searcherManager = new SearcherManager(writerManagerISCH.acquireWriter(), new CustomSearcherFactory(newIndexSchema.getInitializedSchema().getBm25Similarity()));
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
                    this.searcherManager = new SearcherManager(directory, new CustomSearcherFactory(newIndexSchema.getInitializedSchema().getBm25Similarity()));
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

    @Override
    public void initService() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                searcherManager.maybeRefreshBlocking();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, 5, 10, TimeUnit.SECONDS);
    }
}
