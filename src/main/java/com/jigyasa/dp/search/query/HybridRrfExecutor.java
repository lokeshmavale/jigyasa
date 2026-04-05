package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.VectorQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

/**
 * Executes hybrid BM25 + KNN search with Reciprocal Rank Fusion.
 * Handles its own filter/tenant combination because KNN pre-filters
 * must be baked into the KnnFloatVectorQuery constructor.
 *
 * Text query building is delegated to BaseQueryBuilder static methods (DRY).
 */
public class HybridRrfExecutor {

    private static final int RRF_K = 60;

    private final QueryPipeline pipeline;

    public HybridRrfExecutor(QueryPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public TopDocs execute(IndexSearcher searcher, QueryContext context, int numHits) throws IOException {
        QueryRequest req = context.request();
        InitializedIndexSchema schema = context.initializedSchema();

        // Build combined filter (user filters + tenant) — single source of truth
        Query filterQuery = BaseQueryBuilder.buildCombinedFilter(req, schema);

        // BM25 text search — reuse BaseQueryBuilder statics
        Query textQuery = BaseQueryBuilder.resolveTextTypeQuery(req, schema);
        if (filterQuery != null) {
            textQuery = new BooleanQuery.Builder()
                    .add(textQuery, BooleanClause.Occur.MUST)
                    .add(filterQuery, BooleanClause.Occur.FILTER)
                    .build();
        }
        // Apply recency decay via pipeline
        textQuery = pipeline.apply(textQuery, context);
        TopDocs textResults = searcher.search(textQuery, numHits);

        // KNN vector search (filter-only, no scoring modifiers)
        VectorQuery vq = req.getVectorQuery();
        float[] queryVector = BaseQueryBuilder.toFloatArray(vq.getVectorList());
        int k = vq.getK() > 0 ? vq.getK() : numHits;
        Query knnQuery = new KnnFloatVectorQuery(vq.getField(), queryVector, k, filterQuery);
        TopDocs vectorResults = searcher.search(knnQuery, numHits);

        return TopDocs.rrf(numHits, RRF_K, new TopDocs[]{textResults, vectorResults});
    }
}
