package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.utils.ShutdownUtils;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RecoveryCommitServiceISCH implements IndexSchemaChangeHandler {
    private static final Logger log = LoggerFactory.getLogger(RecoveryCommitServiceISCH.class);

    private final IndexWriterManagerISCH writerManager;
    private final TranslogAppenderManager translogAppenderManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile IndexSchema indexSchema;
    private ScheduledFuture<?> commitTask;

    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        this.indexSchema = newIndexSchema;
    }

    @Override
    public void initService() {
        recover();
        startCommitThread();
    }

    private void startCommitThread() {
        commitTask = this.scheduledExecutorService.scheduleWithFixedDelay(this::performCommit, 1, 5, TimeUnit.SECONDS);
    }

    private void performCommit() {
        IndexWriter writer = null;
        try {
            writer = this.writerManager.acquireWriter();
            if (writer.hasUncommittedChanges()) {
                writer.commit();
                this.translogAppenderManager.getAppender().reset();
                log.debug("Commit completed successfully");
            }
        } catch (Exception e) {
            // Log but do NOT rethrow — rethrowing kills scheduleWithFixedDelay permanently
            log.error("Failed to commit data to storage (will retry next cycle)", e);
        } finally {
            if (writer != null) {
                this.writerManager.releaseWriter();
            }
        }
    }

    private void recover() {
        List<IndexRequest> data = this.translogAppenderManager.getAppender().getData();
        if (null == data || data.isEmpty()) {
            log.info("No translog entries to recover");
            return;
        }
        log.info("Starting recovery of {} translog entries", data.size());
        int succeeded = 0;
        int failed = 0;
        List<IndexRequest> failedEntries = new java.util.ArrayList<>();
        for (IndexRequest indexRequest : data) {
            final IndexWriter writer = this.writerManager.acquireWriter();
            try {
                IndexRequestHandler.processIndexRequests(indexRequest, this.indexSchema, writer, null);
                succeeded++;
            } catch (Exception e) {
                failed++;
                failedEntries.add(indexRequest);
                log.error("Recovery failed for translog entry {}/{}, skipping", succeeded + failed, data.size(), e);
            } finally {
                this.writerManager.releaseWriter();
            }
        }
        log.info("Recovery completed: {} succeeded, {} failed out of {} entries", succeeded, failed, data.size());
        if (succeeded > 0 && failed == 0) {
            performCommit();
        } else if (succeeded > 0) {
            // Commit succeeded entries, then rewrite translog with only failed entries
            // to prevent replay storms on subsequent restarts.
            try {
                final IndexWriter writer = this.writerManager.acquireWriter();
                try {
                    writer.commit();
                } finally {
                    this.writerManager.releaseWriter();
                }
                // Rewrite translog with only failed entries
                this.translogAppenderManager.getAppender().reset();
                for (IndexRequest failedReq : failedEntries) {
                    this.translogAppenderManager.getAppender().append(failedReq);
                }
                log.warn("Committed {} recovered entries, rewrote translog with {} failed entries",
                        succeeded, failed);
            } catch (Exception e) {
                log.error("Failed to commit/rewrite translog after partial recovery", e);
            }
        }
    }

    public void shutdown() {
        if (commitTask != null) {
            commitTask.cancel(false);
        }
        scheduledExecutorService.shutdown();
        ShutdownUtils.shutdownAndAwait(scheduledExecutorService, "Commit executor", 10);
        log.info("Recovery/commit service shut down");
    }
}
