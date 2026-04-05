package com.jigyasa.dp.search.collections;

import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.IndexWriterManagerISCH;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.handlers.RecoveryCommitServiceISCH;
import com.jigyasa.dp.search.handlers.TranslogAppenderManager;
import com.jigyasa.dp.search.handlers.TtlSweeperService;
import com.jigyasa.dp.search.handlers.translog.FileAppender;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.ServerMode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-collection lifecycle unit. Each collection has its own:
 * - Lucene index directory (IndexWriterManager + IndexSearcherManager)
 * - Schema (IndexSchemaManager)
 * - Translog
 * - TTL sweeper
 *
 * Created lazily by CollectionRegistry on first access.
 */
public class CollectionContext {
    private static final Logger log = LoggerFactory.getLogger(CollectionContext.class);

    @Getter private final String name;
    @Getter private final HandlerHelpers helpers;
    @Getter private final IndexSchemaManager schemaManager;
    @Getter private final TtlSweeperService ttlSweeper;
    @Getter private final RecoveryCommitServiceISCH commitService;

    private final IndexWriterManagerISCH writerManager;
    private final IndexSearcherManagerISCH searcherManager;
    private final ServerMode serverMode;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    CollectionContext(String name, ServerMode serverMode,
                      String indexDir, String translogDir) {
        this.name = name;
        this.serverMode = serverMode;

        // Create per-collection infrastructure
        this.writerManager = new IndexWriterManagerISCH(indexDir);
        this.searcherManager = new IndexSearcherManagerISCH(serverMode, writerManager, indexDir);
        TranslogAppenderManager translogManager = new TranslogAppenderManager(translogDir);
        InitializedSchemaISCH initializedSchemaISCH = new InitializedSchemaISCH();
        this.commitService = new RecoveryCommitServiceISCH(
                writerManager, translogManager, Executors.newSingleThreadScheduledExecutor());

        // Wire schema change handlers (same order as ServiceModules)
        this.schemaManager = new IndexSchemaManager();
        schemaManager.addHandler(initializedSchemaISCH);
        if (serverMode == ServerMode.WRITE || serverMode == ServerMode.READ_WRITE) {
            schemaManager.addHandler(translogManager);
            schemaManager.addHandler(writerManager);
            schemaManager.addHandler(commitService);
        }
        if (serverMode == ServerMode.READ_WRITE || serverMode == ServerMode.READ) {
            schemaManager.addHandler(searcherManager);
        }

        this.helpers = new HandlerHelpers(schemaManager, searcherManager, writerManager, translogManager);
        this.ttlSweeper = new TtlSweeperService(
                writerManager, searcherManager, schemaManager,
                Executors.newSingleThreadScheduledExecutor());

        log.info("Collection '{}' context created: indexDir={}, translogDir={}", name, indexDir, translogDir);
    }

    /**
     * Initialize collection with a schema and start background services.
     */
    public void initialize(com.jigyasa.dp.search.models.IndexSchema schema) {
        schemaManager.updateIndexSchema(schema);
        schemaManager.initServices();
        // Only start TTL sweeper if server mode supports reads (sweeper needs SearcherManager)
        if (serverMode == ServerMode.READ_WRITE || serverMode == ServerMode.READ) {
            ttlSweeper.start();
        }
        log.info("Collection '{}' initialized", name);
    }

    /**
     * Shutdown all per-collection resources in correct order.
     */
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) return;
        log.info("Shutting down collection '{}'", name);
        // Each step in its own try/catch — ensure writerManager.shutdown() always runs
        // to release the Lucene write.lock file
        try { ttlSweeper.shutdown(); } catch (Exception e) {
            log.error("Error shutting down TTL sweeper for collection '{}'", name, e);
        }
        try { commitService.shutdown(); } catch (Exception e) {
            log.error("Error shutting down commit service for collection '{}'", name, e);
        }
        // Final commit before closing translog — ensures buffered data is committed
        // and translog is reset, so restart doesn't replay already-committed entries
        try {
            org.apache.lucene.index.IndexWriter writer = writerManager.getWriter();
            if (writer != null && writer.isOpen() && writer.hasUncommittedChanges()) {
                writer.commit();
                TranslogAppenderManager tam = helpers.translogAppenderManager();
                if (tam.getAppender() != null) {
                    tam.getAppender().reset();
                }
                log.info("Final commit completed for collection '{}'", name);
            }
        } catch (Exception e) {
            log.error("Error during final commit for collection '{}'", name, e);
        }
        try {
            TranslogAppenderManager tam = helpers.translogAppenderManager();
            if (tam.getAppender() instanceof FileAppender fa) {
                fa.shutdown();
            }
        } catch (Exception e) {
            log.error("Error shutting down translog appender for collection '{}'", name, e);
        }
        try { searcherManager.shutdown(); } catch (Exception e) {
            log.error("Error shutting down searcher manager for collection '{}'", name, e);
        }
        try { writerManager.shutdown(); } catch (Exception e) {
            log.error("Error shutting down writer manager for collection '{}'", name, e);
        }
    }

    /**
     * Returns health information for this collection.
     */
    public com.jigyasa.dp.search.protocol.CollectionHealth getHealth() {
        com.jigyasa.dp.search.protocol.CollectionHealth.Builder health =
                com.jigyasa.dp.search.protocol.CollectionHealth.newBuilder().setName(name);

        try {
            org.apache.lucene.index.IndexWriter writer = writerManager.getWriter();
            health.setWriterOpen(writer != null && writer.isOpen());
            if (writer != null && writer.isOpen()) {
                health.setDocCount(writer.getDocStats().numDocs);
            }
        } catch (Exception e) {
            health.setWriterOpen(false);
        }

        try {
            org.apache.lucene.search.IndexSearcher searcher = searcherManager.acquireSearcher();
            try {
                health.setSearcherAvailable(true);
                if (searcher.getIndexReader() instanceof org.apache.lucene.index.DirectoryReader dr) {
                    health.setSegmentCount(dr.leaves().size());
                }
            } finally {
                searcherManager.releaseSearcher(searcher);
            }
        } catch (Exception e) {
            health.setSearcherAvailable(false);
        }

        return health.build();
    }
}
