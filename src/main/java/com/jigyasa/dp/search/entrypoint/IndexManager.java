package com.jigyasa.dp.search.entrypoint;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.protocol.*;
import com.jigyasa.dp.search.utils.SchemaUtil;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles collection lifecycle operations: create, close, open, list.
 * Delegates to CollectionRegistry for state management.
 */
@RequiredArgsConstructor
public class IndexManager {
    private static final Logger log = LoggerFactory.getLogger(IndexManager.class);

    private final CollectionRegistry registry;

    public void createCollection(CreateCollectionRequest req,
                                 StreamObserver<CreateCollectionResponse> observer) {
        try {
            if (req.getCollection().isEmpty()) {
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription("Collection name is required")
                        .asRuntimeException());
                return;
            }
            IndexSchema schema = SchemaUtil.parseSchema(req.getIndexSchema());
            registry.createCollection(req.getCollection(), schema);
            observer.onNext(CreateCollectionResponse.newBuilder().build());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("CreateCollection failed", e);
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    public void closeCollection(CloseCollectionRequest req,
                                StreamObserver<CloseCollectionResponse> observer) {
        try {
            if (CollectionRegistry.DEFAULT_COLLECTION.equals(
                    CollectionRegistry.resolveCollectionName(req.getCollection()))) {
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription("Cannot close the default collection")
                        .asRuntimeException());
                return;
            }
            registry.closeCollection(req.getCollection());
            observer.onNext(CloseCollectionResponse.newBuilder().build());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            observer.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("CloseCollection failed", e);
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    public void openCollection(OpenCollectionRequest req,
                               StreamObserver<OpenCollectionResponse> observer) {
        try {
            if (req.getCollection().isEmpty()) {
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription("Collection name is required")
                        .asRuntimeException());
                return;
            }
            // Schema is optional for open — if not provided, read from persisted index data
            IndexSchema schema = req.getIndexSchema().isEmpty()
                    ? null
                    : SchemaUtil.parseSchema(req.getIndexSchema());
            registry.openCollection(req.getCollection(), schema);
            observer.onNext(OpenCollectionResponse.newBuilder().build());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("OpenCollection failed", e);
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    public void listCollections(ListCollectionsRequest req,
                                StreamObserver<ListCollectionsResponse> observer) {
        observer.onNext(ListCollectionsResponse.newBuilder()
                .addAllCollections(registry.listCollections())
                .build());
        observer.onCompleted();
    }

    public void health(HealthRequest req, StreamObserver<HealthResponse> observer) {
        try {
            HealthResponse.Builder response = HealthResponse.newBuilder()
                    .setStatus(HealthResponse.Status.SERVING)
                    .addAllCollections(registry.getHealthForAll());
            observer.onNext(response.build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onNext(HealthResponse.newBuilder()
                    .setStatus(HealthResponse.Status.NOT_SERVING)
                    .build());
            observer.onCompleted();
        }
    }

    public void forceMerge(ForceMergeRequest req, StreamObserver<ForceMergeResponse> observer) {
        try {
            var helpers = registry.resolveHelpers(req.getCollection());
            var writerManager = helpers.getIndexWriterManager();
            // Snapshot segment count before merge
            int segsBefore = 0;
            var searcherMgr = helpers.getIndexSearcherManager();
            var preMergeSearcher = searcherMgr.acquireSearcher();
            try {
                if (preMergeSearcher.getIndexReader() instanceof org.apache.lucene.index.DirectoryReader dr) {
                    segsBefore = dr.leaves().size();
                }
            } finally {
                searcherMgr.releaseSearcher(preMergeSearcher);
            }

            var writer = writerManager.acquireWriter();
            try {
                int maxSegments = req.getMaxSegments() > 0 ? req.getMaxSegments() : 1;
                writer.forceMerge(maxSegments);
                writer.commit();

                // Get segment count after merge
                searcherMgr.waitForGeneration(writer.getMaxCompletedSequenceNumber());
                int segsAfter = 0;
                var searcher = searcherMgr.acquireSearcher();
                try {
                    if (searcher.getIndexReader() instanceof org.apache.lucene.index.DirectoryReader dr) {
                        segsAfter = dr.leaves().size();
                    }
                } finally {
                    searcherMgr.releaseSearcher(searcher);
                }

                observer.onNext(ForceMergeResponse.newBuilder()
                        .setSegmentsBefore(segsBefore)
                        .setSegmentsAfter(segsAfter)
                        .build());
                observer.onCompleted();
            } finally {
                writerManager.releaseWriter();
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("ForceMerge failed", e);
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    public void count(CountRequest req, StreamObserver<CountResponse> observer) {
        try {
            var helpers = registry.resolveHelpers(req.getCollection());
            var schema = helpers.getIndexSchemaManager().getIndexSchema().getInitializedSchema();
            var searcherManager = helpers.getIndexSearcherManager();
            var searcher = searcherManager.acquireSearcher();
            try {
                // Build filter query (same as Query RPC filters)
                org.apache.lucene.search.Query query = new org.apache.lucene.search.MatchAllDocsQuery();
                var filterQuery = com.jigyasa.dp.search.utils.FilterQueryBuilder.buildFilterQuery(
                        req.getFiltersList(), schema);
                
                // Add tenant filter if present
                org.apache.lucene.search.Query tenantFilter = null;
                if (!req.getTenantId().isEmpty()) {
                    tenantFilter = new org.apache.lucene.search.TermQuery(
                            new org.apache.lucene.index.Term(
                                    com.jigyasa.dp.search.utils.SystemFields.TENANT_ID, req.getTenantId()));
                }

                if (filterQuery != null || tenantFilter != null) {
                    var boolBuilder = new org.apache.lucene.search.BooleanQuery.Builder()
                            .add(query, org.apache.lucene.search.BooleanClause.Occur.MUST);
                    if (filterQuery != null) {
                        boolBuilder.add(filterQuery, org.apache.lucene.search.BooleanClause.Occur.FILTER);
                    }
                    if (tenantFilter != null) {
                        boolBuilder.add(tenantFilter, org.apache.lucene.search.BooleanClause.Occur.FILTER);
                    }
                    query = boolBuilder.build();
                }

                long count = searcher.count(query);
                observer.onNext(CountResponse.newBuilder().setCount(count).build());
                observer.onCompleted();
            } finally {
                searcherManager.releaseSearcher(searcher);
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("Count failed", e);
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
