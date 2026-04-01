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

    @Override
    public void internalHandle(QueryRequest req, StreamObserver<QueryResponse> observer) {
        HandlerHelpers helpers = registry.resolveHelpers(req.getCollection());
        IndexSearcher indexSearcher = null;
        try {
            IndexSchema indexSchema = helpers.getIndexSchemaManager().getIndexSchema();
            InitializedIndexSchema schema = indexSchema.getInitializedSchema();
            indexSearcher = helpers.getIndexSearcherManager().acquireSearcher();
            QueryContext context = new QueryContext(req, indexSchema, schema);

            int topK = resolveTopK(req.getTopK());
            boolean useSearchAfter = req.hasSearchAfter();
            int numHits = useSearchAfter ? topK : topK + Math.max(0, req.getOffset());
            int offset = useSearchAfter ? 0 : Math.max(0, req.getOffset());

            TopDocs topDocs;
            if (queryBuilder.isHybrid(req)) {
                topDocs = hybridExecutor.execute(indexSearcher, context, numHits);
            } else {
                Query query = queryBuilder.build(context);
                query = pipeline.apply(query, context);
                Sort sort = SortBuilder.build(req.getSortList(), schema);
                topDocs = executor.execute(indexSearcher, query, numHits, sort,
                        useSearchAfter ? req.getSearchAfter() : null);
            }

            QueryResponse response = responseBuilder.build(topDocs, indexSearcher, schema,
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
        } finally {
            if (indexSearcher != null) {
                helpers.getIndexSearcherManager().releaseSearcher(indexSearcher);
            }
        }
    }

    private static int resolveTopK(int requested) {
        if (requested <= 0) return DEFAULT_TOP_K;
        return Math.min(requested, MAX_TOP_K);
    }
}
