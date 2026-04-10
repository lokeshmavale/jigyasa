package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.FacetResult;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.query.BaseQueryBuilder;
import com.jigyasa.dp.search.query.FacetExecutor;
import com.jigyasa.dp.search.query.HybridRrfExecutor;
import com.jigyasa.dp.search.query.QueryContext;
import com.jigyasa.dp.search.query.QueryExecutor;
import com.jigyasa.dp.search.query.QueryModifier;
import com.jigyasa.dp.search.query.QueryPipeline;
import com.jigyasa.dp.search.query.QueryResponseBuilder;
import com.jigyasa.dp.search.query.RecencyDecayModifier;
import com.jigyasa.dp.search.query.SortBuilder;
import com.jigyasa.dp.search.services.RequestHandlerBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private final FacetExecutor facetExecutor;

    public QueryRequestHandler(CollectionRegistry registry) {
        super("Query");
        this.registry = registry;
        this.queryBuilder = new BaseQueryBuilder();
        this.executor = new QueryExecutor();
        this.responseBuilder = new QueryResponseBuilder();
        this.facetExecutor = new FacetExecutor();

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
        try (var lease = helpers.indexSearcherManager().leaseSearcher()) {
            IndexSchema indexSchema = helpers.indexSchemaManager().getIndexSchema();
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

            boolean hasFacets = req.getFacetsCount() > 0;
            FacetsCollector fc = null;
            TopDocs topDocs;

            if (queryBuilder.isHybrid(req)) {
                if (hasFacets) {
                    throw new IllegalArgumentException(
                            "Faceting is not supported with hybrid search. "
                                    + "Use text_query or vector_query alone, not both.");
                }
                topDocs = hybridExecutor.execute(lease.searcher(), context, numHits);
            } else {
                Query query = queryBuilder.build(context);
                query = pipeline.apply(query, context);
                Sort sort = SortBuilder.build(req.getSortList(), schema);

                if (hasFacets && !(query instanceof MatchAllDocsQuery)) {
                    // Filtered path: single-pass TopDocs + FacetsCollector
                    var facetsResult = executor.executeWithFacets(lease.searcher(), query, numHits, sort,
                            useSearchAfter ? req.getSearchAfter() : null);
                    topDocs = facetsResult.topDocs();
                    fc = facetsResult.facetsCollector();
                } else {
                    // No facets, or MatchAll (facets computed via full DV scan, fc stays null)
                    topDocs = executor.execute(lease.searcher(), query, numHits, sort,
                            useSearchAfter ? req.getSearchAfter() : null);
                }
            }

            // Compute facets if requested
            Map<String, FacetResult> facets = Collections.emptyMap();
            if (hasFacets) {
                facets = facetExecutor.compute(lease.searcher(), fc, schema, req.getFacetsList());
            }

            QueryResponse response = responseBuilder.build(topDocs, lease.searcher(), schema,
                    req.getIncludeSource(), offset, topK, req.getMinScore(),
                    req.getSourceFieldsList(), facets);
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
