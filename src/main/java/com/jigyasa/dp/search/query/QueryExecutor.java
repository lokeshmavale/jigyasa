package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.protocol.SearchAfterToken;
import org.apache.lucene.search.*;
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
        return switch (type) {
            case INT -> Integer.parseInt(value);
            case LONG -> Long.parseLong(value);
            case DOUBLE -> Double.parseDouble(value);
            case FLOAT, SCORE -> Float.parseFloat(value);
            case STRING -> new BytesRef(
                    value.getBytes(StandardCharsets.UTF_8));
            default -> value;
        };
    }

    private boolean hasValidToken(SearchAfterToken token) {
        return token.getDocId() >= 0;
    }
}
