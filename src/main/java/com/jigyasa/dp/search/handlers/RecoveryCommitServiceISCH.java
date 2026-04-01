package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.protocol.IndexRequest;
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
    private IndexSchema indexSchema;
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
        final IndexWriter writer = this.writerManager.acquireWriter();
        try {
            if (writer.hasUncommittedChanges()) {
                writer.commit();
                this.translogAppenderManager.getAppender().reset();
                log.debug("Commit completed successfully");
            }
        } catch (Exception e) {
            log.error("Failed to commit data to storage", e);
            throw new RuntimeException("Failed to commit data to storage", e);
        } finally {
            this.writerManager.releaseWriter();
        }
    }

    private void recover() {
        List<IndexRequest> data = this.translogAppenderManager.getAppender().getData();
        if (null == data || data.isEmpty()) {
            log.info("No translog entries to recover");
            return;
        }
        log.info("Starting recovery of {} translog entries", data.size());
        try {
            for (IndexRequest indexRequest : data) {
                final IndexWriter writer = this.writerManager.acquireWriter();
                try {
                    IndexRequestHandler.processIndexRequests(indexRequest, this.indexSchema, writer, null);
                } finally {
                    this.writerManager.releaseWriter();
                }
            }
            log.info("Recovery completed for {} requests", data.size());
            performCommit();
        } catch (Exception e) {
            log.error("Failed to perform recovery", e);
            throw new RuntimeException("Failed to perform recovery", e);
        }
    }

    public void shutdown() {
        if (commitTask != null) {
            commitTask.cancel(false);
        }
        scheduledExecutorService.shutdown();
        log.info("Recovery/commit service shut down");
    }
}
