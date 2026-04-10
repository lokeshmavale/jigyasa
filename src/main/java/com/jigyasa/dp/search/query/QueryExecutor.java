package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.protocol.SearchAfterToken;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encapsulates search execution strategy: standard, searchAfter, or sorted variants.
 */
public class QueryExecutor {

    public TopDocs execute(IndexSearcher searcher, Query query, int numHits,
                           Sort sort, SearchAfterToken searchAfter) throws IOException {
        if (searchAfter != null && hasValidToken(searchAfter)) {
            ScoreDoc after = reconstructScoreDoc(searchAfter, sort);
            return (sort != null)
                    ? searcher.searchAfter(after, query, numHits, sort)
                    : searcher.searchAfter(after, query, numHits);
        }
        return (sort != null)
                ? searcher.search(query, numHits, sort)
                : searcher.search(query, numHits);
    }

    /**
     * Reconstructs the appropriate ScoreDoc subclass from the search-after token.
     * For sorted queries, Lucene requires a FieldDoc (carries sort field values).
     * For relevance-only queries, a plain ScoreDoc suffices.
     */
    private ScoreDoc reconstructScoreDoc(SearchAfterToken token, Sort sort) {
        if (sort != null && token.getSortFieldValuesCount() > 0) {
            SortField[] sortFields = sort.getSort();
            List<String> serialized = token.getSortFieldValuesList();
            Object[] fields = new Object[serialized.size()];
            for (int i = 0; i < serialized.size(); i++) {
                fields[i] = deserializeSortValue(serialized.get(i),
                        i < sortFields.length ? sortFields[i].getType() : SortField.Type.STRING);
            }
            return new FieldDoc(token.getDocId(), token.getScore(), fields);
        }
        return new ScoreDoc(token.getDocId(), token.getScore());
    }

    /**
     * Deserializes a stringified sort value back to the correct Java type
     * matching the Lucene SortField.Type.
     */
    private Object deserializeSortValue(String value, SortField.Type type) {
        if (value.isEmpty()) return null;
        try {
            return switch (type) {
                case INT -> Integer.parseInt(value);
                case LONG -> Long.parseLong(value);
                case DOUBLE -> Double.parseDouble(value);
                case FLOAT, SCORE -> Float.parseFloat(value);
                case STRING -> new BytesRef(
                        value.getBytes(StandardCharsets.UTF_8));
                default -> value;
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid search_after token value '" + value + "' for sort type " + type, e);
        }
    }

    /**
     * Executes search with FacetsCollector in a single pass using FacetsCollectorManager.
     * Returns both TopDocs and the populated FacetsCollector.
     * Preserves Sort and SearchAfter behavior via Lucene's built-in support.
     *
     * IMPORTANT: Facet counts always reflect ALL matching docs (not just the page).
     * When searchAfter is used, we run a separate non-paged search for facets so that
     * the FacetsCollector sees the full result set.
     */
    public FacetsCollectorManager.FacetsResult executeWithFacets(
            IndexSearcher searcher, Query query, int numHits,
            Sort sort, SearchAfterToken searchAfter) throws IOException {
        FacetsCollectorManager fcManager = new FacetsCollectorManager();

        if (searchAfter != null && hasValidToken(searchAfter)) {
            // searchAfter paging: facets must still reflect ALL matches.
            // Run a full (non-paged) facet collection, then a paged TopDocs search.
            FacetsCollectorManager.FacetsResult fullResult =
                    (sort != null)
                            ? FacetsCollectorManager.search(searcher, query, numHits, sort, fcManager)
                            : FacetsCollectorManager.search(searcher, query, numHits, fcManager);

            // Now run the paged search for correct TopDocs
            ScoreDoc after = reconstructScoreDoc(searchAfter, sort);
            TopDocs pagedTopDocs = (sort != null)
                    ? searcher.searchAfter(after, query, numHits, sort)
                    : searcher.searchAfter(after, query, numHits);

            return new FacetsCollectorManager.FacetsResult(pagedTopDocs, fullResult.facetsCollector());
        }

        return (sort != null)
                ? FacetsCollectorManager.search(searcher, query, numHits, sort, fcManager)
                : FacetsCollectorManager.search(searcher, query, numHits, fcManager);
    }

    private boolean hasValidToken(SearchAfterToken token) {
        return token.getDocId() >= 0;
    }
}
