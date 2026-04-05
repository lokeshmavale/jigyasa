package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.query.*;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Thin orchestrator that delegates to composable pipeline components.
 * No query logic lives here — it coordinates:
 *   BaseQueryBuilder → QueryPipeline (modifiers) → QueryExecutor → QueryResponseBuilder
 */
public class QueryRequestHandler extends RequestHandlerBase<QueryRequest, QueryResponse> {
    private static final Logger log = LoggerFactory.getLogger(QueryRequestHandler.class);
    private static final int DEFAULT_TOP_K = 10;
    private static final int MAX_TOP_K = 10000;

    private final CollectionRegistry registry;
    private final BaseQueryBuilder queryBuilder;
    private final QueryPipeline pipeline;
    private final QueryPipeline hybridPipeline;
    private final QueryExecutor executor;
    private final HybridRrfExecutor hybridExecutor;
    private final QueryResponseBuilder responseBuilder;

    public QueryRequestHandler(CollectionRegistry registry) {
        super("Query");
        this.registry = registry;
        this.queryBuilder = new BaseQueryBuilder();
        this.executor = new QueryExecutor();
        this.responseBuilder = new QueryResponseBuilder();

        // Pipeline: only scoring modifiers. Filters (user + tenant) are handled
        // in BaseQueryBuilder.build() because KNN pre-filters must be baked into
        // the KnnFloatVectorQuery constructor — wrapping with outer BooleanQuery
        // causes ANN to search wrong vector space.
        List<QueryModifier> modifiers = List.of(
                new RecencyDecayModifier()
        );
        this.pipeline = new QueryPipeline(modifiers);

        // Hybrid pipeline: same scoring modifiers (filters/tenant handled inside HybridRrfExecutor)
        this.hybridPipeline = new QueryPipeline(List.of(new RecencyDecayModifier()));
        this.hybridExecutor = new HybridRrfExecutor(hybridPipeline);
    }

    private static final int MAX_OFFSET = 10_000;

    @Override
    public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
        HandlerHelpers helpers = registry.resolveHelpers(req.getCollection());
        try (var lease = helpers.getIndexSearcherManager().leaseSearcher()) {
            IndexSchema indexSchema = helpers.getIndexSchemaManager().getIndexSchema();
            InitializedIndexSchema schema = indexSchema.getInitializedSchema();
            QueryContext context = new QueryContext(req, indexSchema, schema);

            int topK = resolveTopK(req.getTopK());
            int offset = Math.max(0, req.getOffset());
            if (offset > MAX_OFFSET) {
                throw new IllegalArgumentException("offset must be <= " + MAX_OFFSET + ", got: " + offset);
            }
            if (topK > MAX_TOP_K) {
                topK = MAX_TOP_K;
            }
            boolean useSearchAfter = req.hasSearchAfter();
            int numHits = useSearchAfter ? topK : topK + offset;
            if (useSearchAfter) offset = 0;

            TopDocs topDocs;
            if (queryBuilder.isHybrid(req)) {
                topDocs = hybridExecutor.execute(lease.searcher(), context, numHits);
            } else {
                Query query = queryBuilder.build(context);
                query = pipeline.apply(query, context);
                Sort sort = SortBuilder.build(req.getSortList(), schema);
                topDocs = executor.execute(lease.searcher(), query, numHits, sort,
                        useSearchAfter ? req.getSearchAfter() : null);
            }

            QueryResponse response = responseBuilder.build(topDocs, lease.searcher(), schema,
                    req.getIncludeSource(), offset, topK, req.getMinScore(),
                    req.getSourceFieldsList());
            observer.onNext(response);
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn("Invalid query request: {}", e.getMessage());
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("Query execution failed", e);
            observer.onError(Status.INTERNAL.withDescription("Query execution failed: " + e.getMessage()).asRuntimeException());
        }
    }

    private static int resolveTopK(int requested) {
        if (requested <= 0) return DEFAULT_TOP_K;
        return Math.min(requested, MAX_TOP_K);
    }
}
