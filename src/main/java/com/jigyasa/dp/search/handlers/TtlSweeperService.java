package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.utils.ShutdownUtils;
import com.jigyasa.dp.search.utils.SystemFields;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Background service that periodically deletes expired documents based on _ttl_expires_at.
 * Only active when schema has ttlEnabled=true.
 * Documents with _ttl_expires_at == 0 never expire (SEMANTIC tier default).
 * Documents with _ttl_expires_at > 0 and <= now are deleted.
 */
public class TtlSweeperService {
    private static final Logger log = LoggerFactory.getLogger(TtlSweeperService.class);
    private static final long SWEEP_INTERVAL_SECS = 30;

    private final IndexWriterManagerISCH writerManager;
    private final IndexSearcherManagerISCH searcherManager;
    private final IndexSchemaManager schemaManager;
    private final ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> sweepTask;

    public TtlSweeperService(IndexWriterManagerISCH writerManager,
                             IndexSearcherManagerISCH searcherManager,
                             IndexSchemaManager schemaManager,
                             ScheduledExecutorService executor) {
        this.writerManager = writerManager;
        this.searcherManager = searcherManager;
        this.schemaManager = schemaManager;
        this.executor = executor;
    }

    public void start() {
        sweepTask = executor.scheduleWithFixedDelay(
                this::sweep, SWEEP_INTERVAL_SECS, SWEEP_INTERVAL_SECS, TimeUnit.SECONDS);
        log.info("TTL sweeper started (interval={}s)", SWEEP_INTERVAL_SECS);
    }

    public void shutdown() {
        if (sweepTask != null) {
            sweepTask.cancel(false);
        }
        executor.shutdown();
        ShutdownUtils.shutdownAndAwait(executor, "TTL sweeper", 5);
        log.info("TTL sweeper stopped");
    }

    private void sweep() {
        try {
            // Skip if TTL not enabled in current schema
            if (schemaManager.getIndexSchema() == null || !schemaManager.getIndexSchema().isTtlEnabled()) {
                return;
            }

            long now = System.currentTimeMillis();
            Query expiredQuery = LongPoint.newRangeQuery(SystemFields.TTL_EXPIRES_AT, 1L, now);

            // Delete expired docs, release writer lock BEFORE waiting for NRT visibility
            long seqNo;
            IndexWriter writer = writerManager.acquireWriter();
            try {
                seqNo = writer.deleteDocuments(expiredQuery);
            } finally {
                writerManager.releaseWriter();
            }

            // Wait for deletion visibility outside the writer lock
            if (seqNo >= 0) {
                searcherManager.waitForGeneration(seqNo);
            }
            log.debug("TTL sweep completed at {}", now);
        } catch (Exception e) {
            log.warn("TTL sweep failed, will retry next cycle", e);
        }
    }
}
