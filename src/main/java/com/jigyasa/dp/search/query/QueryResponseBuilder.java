package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.SearchAfterToken;
import com.jigyasa.dp.search.utils.SourceVisitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
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
        QueryResponse.Builder response = QueryResponse.newBuilder();
        response.setTotalHits(topDocs.totalHits.value());
        response.setTotalHitsExact(
                minScore <= 0.0f && topDocs.totalHits.relation() == TotalHits.Relation.EQUAL_TO);

        boolean hasProjection = sourceFields != null && !sourceFields.isEmpty();
        Set<String> projectedFields = hasProjection ? Set.copyOf(sourceFields) : null;

        String keyFieldName = schema.getKeyFieldName();
        int end = (int) Math.min((long) offset + topK, topDocs.scoreDocs.length);
        int filteredCount = 0;

        for (int i = offset; i < end; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];

            // Skip hits below min_score threshold
            if (minScore > 0.0f && scoreDoc.score < minScore) {
                filteredCount++;
                continue;
            }

            QueryHit.Builder hit = QueryHit.newBuilder();
            hit.setScore(scoreDoc.score);

            SourceVisitor visitor = new SourceVisitor();
            searcher.storedFields().document(scoreDoc.doc, visitor);

            if (visitor.getSrc() != null) {
                String srcJson = new String(visitor.getSrc(), StandardCharsets.UTF_8);
                JsonNode parsedSource = null;

                // Parse JSON once, reuse for key extraction and projection
                try {
                    parsedSource = OBJECT_MAPPER.readTree(srcJson);
                } catch (Exception e) {
                    log.warn("Failed to parse source JSON", e);
                }

                if (parsedSource != null) {
                    JsonNode keyNode = parsedSource.get(keyFieldName);
                    hit.setDocId(keyNode != null ? keyNode.asText() : "");
                } else {
                    hit.setDocId("");
                }

                if (hasProjection && parsedSource != null) {
                    hit.setSource(projectFromParsed(parsedSource, projectedFields));
                } else if (includeSource) {
                    hit.setSource(srcJson);
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

        return response.build();
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
