package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.DeleteByQueryRequest;
import com.jigyasa.dp.search.protocol.DeleteByQueryResponse;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import com.jigyasa.dp.search.utils.FilterQueryBuilder;
import com.jigyasa.dp.search.utils.SystemFields;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteByQueryRequestHandler extends RequestHandlerBase<DeleteByQueryRequest, DeleteByQueryResponse> {
    private static final Logger log = LoggerFactory.getLogger(DeleteByQueryRequestHandler.class);
    private final CollectionRegistry registry;

    public DeleteByQueryRequestHandler(CollectionRegistry registry) {
        super("DeleteByQuery");
        this.registry = registry;
    }

    @Override
    public void internalHandle(DeleteByQueryRequest req, StreamObserver<DeleteByQueryResponse> observer) {
        HandlerHelpers helpers = registry.resolveHelpers(req.getCollection());
        if (req.getFiltersList().isEmpty()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("At least one filter is required — refusing to delete all documents")
                    .asRuntimeException());
            return;
        }

        InitializedIndexSchema schema = helpers.getIndexSchemaManager().getIndexSchema().getInitializedSchema();
        Query filterQuery = FilterQueryBuilder.buildFilterQuery(req.getFiltersList(), schema);
        if (filterQuery == null) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("Filter query resolved to null")
                    .asRuntimeException());
            return;
        }

        // Scope to tenant if specified
        if (!req.getTenantId().isEmpty()) {
            Query tenantQuery = new org.apache.lucene.search.TermQuery(
                    new org.apache.lucene.index.Term(SystemFields.TENANT_ID, req.getTenantId()));
            filterQuery = new org.apache.lucene.search.BooleanQuery.Builder()
                    .add(filterQuery, org.apache.lucene.search.BooleanClause.Occur.FILTER)
                    .add(tenantQuery, org.apache.lucene.search.BooleanClause.Occur.FILTER)
                    .build();
        }

        try (var lease = helpers.getIndexWriterManager().leaseWriter()) {
            long seqNo = lease.writer().deleteDocuments(filterQuery);
            // Force commit to make delete durable — delete-by-query is not translog'd,
            // so without commit, crash before next periodic commit would resurrect deleted docs.
            lease.writer().commit();
            // Reset translog so recovery doesn't replay index ops that would re-add deleted docs.
            // If reset fails, log error but still return success — the commit is durable and
            // the periodic commit thread will reset translog on its next successful cycle.
            try {
                helpers.getTranslogAppenderManager().getAppender().reset();
            } catch (Exception resetEx) {
                log.error("Translog reset failed after delete-by-query commit; " +
                        "periodic commit will clean up on next cycle", resetEx);
            }
            log.info("DeleteByQuery executed and committed, seqNo={}", seqNo);
            helpers.getIndexSearcherManager().waitForGeneration(seqNo);
            observer.onNext(DeleteByQueryResponse.newBuilder()
                    .setDeletedCount(-1)
                    .build());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn("Invalid delete-by-query request: {}", e.getMessage());
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("DeleteByQuery failed", e);
            observer.onError(Status.INTERNAL.withDescription("DeleteByQuery failed: " + e.getMessage()).asRuntimeException());
        }
    }
}
