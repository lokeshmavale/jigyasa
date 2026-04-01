package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.VectorQuery;
import com.jigyasa.dp.search.utils.FilterQueryBuilder;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constructs the base Lucene Query from a QueryRequest.
 * Handles text-only, vector-only, and match-all (filter-only) cases.
 * Hybrid (text + vector) is handled separately by HybridRrfExecutor.
 *
 * Filters (user filters + tenant) are applied HERE, not via pipeline modifiers,
 * because KNN pre-filters must be baked into the KnnFloatVectorQuery constructor.
 * Wrapping KNN with an outer BooleanQuery filter causes the ANN traversal to
 * search the wrong vector space, returning fewer than k results.
 */
public class BaseQueryBuilder {

    private static final ThreadLocal<Map<String, QueryParser>> PARSER_CACHE =
            ThreadLocal.withInitial(HashMap::new);

    public Query build(QueryContext context) {
        QueryRequest req = context.request();
        InitializedIndexSchema schema = context.initializedSchema();
        boolean hasText = !req.getTextQuery().isEmpty();
        boolean hasPhrase = !req.getPhraseQuery().isEmpty();
        boolean hasFuzzy = !req.getFuzzyQuery().isEmpty();
        boolean hasVector = req.hasVectorQuery() && req.getVectorQuery().getVectorCount() > 0;
        boolean hasPrefixQuery = !req.getPrefixQuery().isEmpty();
        boolean hasQueryString = !req.getQueryString().isEmpty();

        // Merge user filters + tenant into a single combined filter
        Query combinedFilter = buildCombinedFilter(req, schema);

        boolean hasAnyText = hasText || hasPhrase || hasFuzzy || hasPrefixQuery || hasQueryString;

        if (!hasAnyText && !hasVector) {
            Query base = new MatchAllDocsQuery();
            if (combinedFilter != null) {
                return new BooleanQuery.Builder()
                        .add(base, BooleanClause.Occur.MUST)
                        .add(combinedFilter, BooleanClause.Occur.FILTER)
                        .build();
            }
            return base;
        }

        if (hasAnyText && !hasVector) {
            Query textTypeQuery = resolveTextTypeQuery(req, schema);
            return combinedFilter != null ? new BooleanQuery.Builder()
                    .add(textTypeQuery, BooleanClause.Occur.MUST)
                    .add(combinedFilter, BooleanClause.Occur.FILTER)
                    .build() : textTypeQuery;
        }

        // Vector-only: tenant MUST be in the KNN pre-filter, not an outer wrapper
        VectorQuery vq = req.getVectorQuery();
        float[] queryVector = toFloatArray(vq.getVectorList());
        int k = vq.getK() > 0 ? vq.getK() : 10;
        return new KnnFloatVectorQuery(vq.getField(), queryVector, k, combinedFilter);
    }

    /**
     * Builds a combined filter from user filters and tenant isolation.
     * Both must be in the KNN pre-filter for correct ANN traversal.
     * Package-access: also used by HybridRrfExecutor for consistency.
     */
    static Query buildCombinedFilter(QueryRequest req, InitializedIndexSchema schema) {
        Query userFilter = FilterQueryBuilder.buildFilterQuery(req.getFiltersList(), schema);
        Query tenantFilter = null;
        if (!req.getTenantId().isEmpty()) {
            tenantFilter = new TermQuery(
                    new org.apache.lucene.index.Term(
                            com.jigyasa.dp.search.utils.SystemFields.TENANT_ID, req.getTenantId()));
        }
        if (userFilter == null && tenantFilter == null) return null;
        if (userFilter == null) return tenantFilter;
        if (tenantFilter == null) return userFilter;
        return new BooleanQuery.Builder()
                .add(userFilter, BooleanClause.Occur.FILTER)
                .add(tenantFilter, BooleanClause.Occur.FILTER)
                .build();
    }

    public boolean isHybrid(QueryRequest req) {
        boolean hasText = !req.getTextQuery().isEmpty();
        boolean hasPhrase = !req.getPhraseQuery().isEmpty();
        boolean hasFuzzy = !req.getFuzzyQuery().isEmpty();
        boolean hasPrefixQuery = !req.getPrefixQuery().isEmpty();
        boolean hasQueryString = !req.getQueryString().isEmpty();
        boolean hasVector = req.hasVectorQuery() && req.getVectorQuery().getVectorCount() > 0;
        return (hasText || hasPhrase || hasFuzzy || hasPrefixQuery || hasQueryString) && hasVector;
    }

    /**
     * Resolves which text-type query to use based on priority:
     * query_string > text > phrase > fuzzy > prefix.
     */
    static Query resolveTextTypeQuery(QueryRequest req, InitializedIndexSchema schema) {
        if (!req.getQueryString().isEmpty()) {
            return buildQueryString(req.getQueryString(), req.getQueryStringDefaultField(), schema);
        }
        if (!req.getTextQuery().isEmpty()) {
            return buildTextQuery(req.getTextQuery(), req.getTextField(), schema);
        }
        if (!req.getPhraseQuery().isEmpty()) {
            return buildPhraseQuery(req.getPhraseQuery(), req.getPhraseField(), req.getPhraseSlop(), schema);
        }
        if (!req.getFuzzyQuery().isEmpty()) {
            return buildFuzzyQuery(req.getFuzzyQuery(), req.getFuzzyField(), req.getMaxEdits(), req.getPrefixLength(), schema);
        }
        return buildPrefixQuery(req.getPrefixQuery(), req.getPrefixField(), schema);
    }

    // ---- Query string parsing (Lucene QueryParser — full syntax) ----

    static Query buildQueryString(String queryString, String defaultField, InitializedIndexSchema schema) {
        String field = defaultField;
        if (field == null || field.isEmpty()) {
            // Use first searchable field as default
            field = schema.getFieldLookupMap().values().stream()
                    .filter(sf -> sf.isSearchable() && isTextField(sf.getType()))
                    .map(SchemaField::getName)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No searchable text field in schema for query_string"));
        }
        try {
            Map<String, QueryParser> cache = PARSER_CACHE.get();
            QueryParser parser = cache.computeIfAbsent(field, f -> new QueryParser(f, schema.getSearchAnalyzer()));
            parser.setAllowLeadingWildcard(false);
            return parser.parse(queryString);
        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new IllegalArgumentException("Invalid query_string syntax: " + e.getMessage(), e);
        }
    }

    // ---- Text query building (shared with HybridRrfExecutor via package access) ----

    static Query buildTextQuery(String queryText, String field, InitializedIndexSchema schema) {
        if (field.isEmpty()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SchemaField sf : schema.getFieldLookupMap().values()) {
                if (sf.isSearchable() && isTextField(sf.getType())) {
                    builder.add(buildTermQueryForField(sf.getName(), queryText), BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }
        return buildTermQueryForField(field, queryText);
    }

    static Query buildTermQueryForField(String fieldName, String queryText) {
        String[] terms = queryText.trim().split("\\s+");
        if (terms.length == 1) {
            return new TermQuery(new Term(fieldName, terms[0].toLowerCase()));
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : terms) {
            builder.add(new TermQuery(new Term(fieldName, term.toLowerCase())), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    static float[] toFloatArray(List<Float> list) {
        float[] arr = new float[list.size()];
        for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
        return arr;
    }

    static Query buildPrefixQuery(String prefix, String field, InitializedIndexSchema schema) {
        if (field.isEmpty()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SchemaField sf : schema.getFieldLookupMap().values()) {
                if (sf.isSearchable() && isTextField(sf.getType())) {
                    builder.add(new PrefixQuery(new Term(sf.getName(), prefix.toLowerCase())), BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }
        return new PrefixQuery(new Term(field, prefix.toLowerCase()));
    }

    // ---- Phrase query building ----

    static Query buildPhraseQuery(String phraseText, String field, int slop, InitializedIndexSchema schema) {
        if (field.isEmpty()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SchemaField sf : schema.getFieldLookupMap().values()) {
                if (sf.isSearchable() && isTextField(sf.getType())) {
                    builder.add(buildPhraseQueryForField(sf.getName(), phraseText, slop), BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }
        return buildPhraseQueryForField(field, phraseText, slop);
    }

    static Query buildPhraseQueryForField(String fieldName, String phraseText, int slop) {
        String trimmed = phraseText.trim().toLowerCase();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Phrase query cannot be empty or whitespace-only");
        }
        if (slop < 0) {
            throw new IllegalArgumentException("phrase_slop must be >= 0, got: " + slop);
        }
        String[] terms = trimmed.split("\\s+");
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        for (String term : terms) {
            builder.add(new Term(fieldName, term));
        }
        return builder.build();
    }

    // ---- Fuzzy query building ----

    static Query buildFuzzyQuery(String fuzzyText, String field, int maxEdits, int prefixLength, InitializedIndexSchema schema) {
        int edits = Math.max(0, Math.min(maxEdits <= 0 ? 2 : maxEdits, 2));
        int prefix = Math.max(0, prefixLength);

        if (field.isEmpty()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (SchemaField sf : schema.getFieldLookupMap().values()) {
                if (sf.isSearchable() && isTextField(sf.getType())) {
                    builder.add(buildFuzzyQueryForField(sf.getName(), fuzzyText, edits, prefix), BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }
        return buildFuzzyQueryForField(field, fuzzyText, edits, prefix);
    }

    static Query buildFuzzyQueryForField(String fieldName, String fuzzyText, int maxEdits, int prefixLength) {
        String trimmed = fuzzyText.trim().toLowerCase();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Fuzzy query cannot be empty or whitespace-only");
        }
        String[] terms = trimmed.split("\\s+");
        if (terms.length == 1) {
            return new FuzzyQuery(new Term(fieldName, terms[0]), maxEdits, prefixLength);
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : terms) {
            builder.add(new FuzzyQuery(new Term(fieldName, term), maxEdits, prefixLength), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    private static boolean isTextField(FieldDataType type) {
        return type == FieldDataType.STRING || type == FieldDataType.STRING_COLLECTION;
    }
}
