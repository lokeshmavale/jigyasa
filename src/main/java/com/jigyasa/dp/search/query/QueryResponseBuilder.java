package com.jigyasa.dp.search.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.FacetResult;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.SearchAfterToken;
import com.jigyasa.dp.search.utils.SourceVisitor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Builds gRPC QueryResponse from Lucene TopDocs.
 * Handles source retrieval, key extraction, and searchAfter cursor generation.
 */
public class QueryResponseBuilder {
    private static final Logger log = LoggerFactory.getLogger(QueryResponseBuilder.class);
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    public QueryResponse build(TopDocs topDocs, IndexSearcher searcher,
                                InitializedIndexSchema schema, boolean includeSource,
                                int offset, int topK) throws Exception {
        return build(topDocs, searcher, schema, includeSource, offset, topK, 0.0f, List.of());
    }

    public QueryResponse build(TopDocs topDocs, IndexSearcher searcher,
                                InitializedIndexSchema schema, boolean includeSource,
                                int offset, int topK, float minScore) throws Exception {
        return build(topDocs, searcher, schema, includeSource, offset, topK, minScore, List.of());
    }

    public QueryResponse build(TopDocs topDocs, IndexSearcher searcher,
                                InitializedIndexSchema schema, boolean includeSource,
                                int offset, int topK, float minScore,
                                List<String> sourceFields) throws Exception {
        return build(topDocs, searcher, schema, includeSource, offset, topK, minScore,
                sourceFields, Collections.emptyMap());
    }

    public QueryResponse build(TopDocs topDocs, IndexSearcher searcher,
                                InitializedIndexSchema schema, boolean includeSource,
                                int offset, int topK, float minScore,
                                List<String> sourceFields,
                                Map<String, FacetResult> facets) throws Exception {
        QueryResponse.Builder response = QueryResponse.newBuilder();
        response.setTotalHits(topDocs.totalHits.value());
        response.setTotalHitsExact(
                minScore <= 0.0f && topDocs.totalHits.relation() == TotalHits.Relation.EQUAL_TO);

        boolean hasProjection = sourceFields != null && !sourceFields.isEmpty();
        Set<String> projectedFields = hasProjection ? Set.copyOf(sourceFields) : null;
        boolean needsFullParse = includeSource || hasProjection;

        String keyFieldName = schema.getKeyFieldName();
        int end = (int) Math.min((long) offset + topK, topDocs.scoreDocs.length);

        // Acquire StoredFields once — avoid per-hit accessor creation
        var storedFields = searcher.storedFields();
        SourceVisitor visitor = new SourceVisitor();

        for (int i = offset; i < end; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];

            if (minScore > 0.0f && scoreDoc.score < minScore) {
                continue;
            }

            QueryHit.Builder hit = QueryHit.newBuilder();
            hit.setScore(scoreDoc.score);

            visitor.reset();
            storedFields.document(scoreDoc.doc, visitor);

            byte[] srcBytes = visitor.getSrc();
            if (srcBytes != null) {
                if (needsFullParse) {
                    // Full parse needed for source/projection — also extract key
                    String srcJson = new String(srcBytes, StandardCharsets.UTF_8);
                    try {
                        JsonNode parsedSource = OBJECT_MAPPER.readTree(srcJson);
                        JsonNode keyNode = parsedSource.get(keyFieldName);
                        hit.setDocId(keyNode != null ? keyNode.asText() : "");

                        if (hasProjection) {
                            hit.setSource(projectFromParsed(parsedSource, projectedFields));
                        } else {
                            hit.setSource(srcJson);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse source JSON", e);
                        hit.setDocId("");
                    }
                } else {
                    // Fast path: extract only the key field without full JSON tree parse
                    hit.setDocId(extractKeyFromBytes(srcBytes, keyFieldName));
                }
            }

            response.addHits(hit.build());
        }

        // SearchAfter cursor for next page
        if (end > 0 && end <= topDocs.scoreDocs.length) {
            ScoreDoc last = topDocs.scoreDocs[end - 1];
            SearchAfterToken.Builder tokenBuilder = SearchAfterToken.newBuilder()
                    .setScore(last.score)
                    .setDocId(last.doc);

            // For sorted queries, ScoreDocs are actually FieldDoc instances.
            // We must serialize the sort field values so the client can reconstruct
            // a valid FieldDoc on the next request (Lucene requires FieldDoc, not ScoreDoc,
            // for searchAfter with Sort).
            if (last instanceof FieldDoc fieldDoc && fieldDoc.fields != null) {
                for (Object field : fieldDoc.fields) {
                    tokenBuilder.addSortFieldValues(serializeSortValue(field));
                }
            }

            response.setNextSearchAfter(tokenBuilder.build());
        }

        // Attach facet results
        if (facets != null && !facets.isEmpty()) {
            response.putAllFacets(facets);
        }

        return response.build();
    }

    /**
     * Extracts a single key field value from raw JSON bytes using Jackson's streaming parser.
     * Avoids building a full JsonNode tree — O(key_position) scan, zero tree allocation.
     */
    private static String extractKeyFromBytes(byte[] srcBytes, String keyFieldName) {
        try {
            com.fasterxml.jackson.core.JsonParser parser = OBJECT_MAPPER.getFactory()
                    .createParser(srcBytes);
            if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                parser.close();
                return "";
            }
            while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
                String name = parser.currentName();
                parser.nextToken(); // move to value
                if (keyFieldName.equals(name)) {
                    String value = parser.getValueAsString("");
                    parser.close();
                    return value;
                }
                parser.skipChildren(); // skip nested objects/arrays
            }
            parser.close();
        } catch (Exception e) {
            // Fallback: full parse
            try {
                JsonNode node = OBJECT_MAPPER.readTree(srcBytes);
                JsonNode keyNode = node.get(keyFieldName);
                return keyNode != null ? keyNode.asText() : "";
            } catch (Exception ignored) {}
        }
        return "";
    }

    private String projectFromParsed(JsonNode root, Set<String> fields) {
        try {
            if (!(root instanceof ObjectNode obj)) return root.toString();
            ObjectNode projected = OBJECT_MAPPER.createObjectNode();
            for (String field : fields) {
                JsonNode value = obj.get(field);
                if (value != null) projected.set(field, value);
            }
            return OBJECT_MAPPER.writeValueAsString(projected);
        } catch (Exception e) {
            log.warn("Failed to project fields", e);
            return root.toString();
        }
    }

    /**
     * Serializes a sort field value to string. BytesRef (used for STRING sorts)
     * must be decoded as UTF-8 — BytesRef.toString() returns a hex diagnostic form.
     */
    private static String serializeSortValue(Object field) {
        if (field == null) return "";
        if (field instanceof BytesRef br) {
            return new String(br.bytes, br.offset, br.length, StandardCharsets.UTF_8);
        }
        return field.toString();
    }

}
