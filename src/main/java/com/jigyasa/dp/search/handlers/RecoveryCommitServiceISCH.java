package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.protocol.IndexRequest;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.IndexWriter;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RecoveryCommitServiceISCH implements IndexSchemaChangeHandler {

    private final IndexWriterManagerISCH writerManager;
    private final TranslogAppenderManager translogAppenderManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private IndexSchema indexSchema;

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
        this.scheduledExecutorService.scheduleWithFixedDelay(this::performCommit, 1, 5, TimeUnit.SECONDS);
    }

    private void performCommit() {
        final IndexWriter writer = this.writerManager.acquireWriter();
        try {
            if (writer.hasUncommittedChanges()) {
                writer.commit();
                this.translogAppenderManager.getAppender().reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to Commit data in storage");
        } finally {
            this.writerManager.releaseWriter();
        }
    }

    private void recover() {
        List<IndexRequest> data = this.translogAppenderManager.getAppender().getData();
        if (null == data || data.isEmpty()) {
            return;
        }
        try {
            System.out.println("Starting recovery");
            for (IndexRequest indexRequest : data) {
                IndexRequestHandler.processIndexRequests(indexRequest, this.indexSchema, this.writerManager.acquireWriter(), null);
            }
            System.out.println("Recovery Completed for " + data.size() + " requests");
            performCommit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to perform recovery", e);
        }

    }
}
