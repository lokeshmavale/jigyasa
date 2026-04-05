package com.jigyasa.dp.search.handlers;

import com.google.common.collect.ImmutableMap;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.text.AnalyzerFactory;
import com.jigyasa.dp.search.text.AnalyzerNames;
import com.jigyasa.dp.search.text.PerFieldAnalyzer;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similarities.BM25Similarity;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Index Schema Initializer
 */
@Getter
public class InitializedSchemaISCH implements IndexSchemaChangeHandler {
    private static final String DEFAULT_ANALYZER = AnalyzerNames.STANDARD;

    /**
     * Initialize and Set Initialized index schema in IndexSchema
     */
    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        final InitializedIndexSchema initializedIndexSchema = new InitializedIndexSchema();

        initFieldMap(newIndexSchema, initializedIndexSchema);
        initAnalyzers(newIndexSchema, initializedIndexSchema);
        initSimilarity(newIndexSchema, initializedIndexSchema);

        newIndexSchema.setInitializedSchema(initializedIndexSchema);
    }

    private void initSimilarity(IndexSchema newIndexSchema, InitializedIndexSchema newSchema) {
        BM25Config bm25Config = newIndexSchema.getBm25Config();
        newSchema.setBm25Similarity(new BM25Similarity(bm25Config.getK1(), bm25Config.getB()));
    }

    private void initAnalyzers(IndexSchema indexSchema, InitializedIndexSchema newSchema) {
        Map<String, Analyzer> indexPerFieldAnalyzer = new LinkedHashMap<>();
        Map<String, Analyzer> searchPerFieldAnalyzer = new LinkedHashMap<>();

        for (SchemaField field : indexSchema.getFields()) {
            if (field.isSearchable()) {
                indexPerFieldAnalyzer.put(field.getName(), AnalyzerFactory.getAnalyzer(StringUtils.defaultIfEmpty(field.getIndexAnalyzer(), DEFAULT_ANALYZER)));
                searchPerFieldAnalyzer.put(field.getName(), AnalyzerFactory.getAnalyzer(StringUtils.defaultIfEmpty(field.getSearchAnalyzer(), DEFAULT_ANALYZER)));
            }
        }

        newSchema.setIndexAnalyzer(new PerFieldAnalyzer(indexPerFieldAnalyzer, newSchema.getFieldLookupMap()));
        newSchema.setSearchAnalyzer(new PerFieldAnalyzer(searchPerFieldAnalyzer, newSchema.getFieldLookupMap()));
    }

    private void initFieldMap(IndexSchema indexSchema, InitializedIndexSchema newSchema) {
        final Map<String, SchemaField> fieldMap = new HashMap<>(indexSchema.getFields().length);
        for (SchemaField field : indexSchema.getFields()) {
            if (field.isKey()) {
                if (newSchema.getKeyField() != null) {
                    throw new IllegalArgumentException("Schema should have single key field, found multiple!");
                }
                newSchema.setKeyField(field);
            }
            fieldMap.put(field.getName(), field);
        }

        if (newSchema.getKeyField() == null) {
            throw new IllegalArgumentException("Schema should have single key field, found none!");
        }

        newSchema.setFieldLookupMap(ImmutableMap.copyOf(fieldMap));
        newSchema.setFieldMapperStrategyMap(ThreadLocal.withInitial(() -> createFieldMapperStrategyMap(fieldMap)));
    }

    private Map<String, FieldMapperStrategy> createFieldMapperStrategyMap(Map<String, SchemaField> fieldMap) {
        final Map<String, FieldMapperStrategy> fieldMapperStrategy = new HashMap<>(fieldMap.size());
        for (Map.Entry<String, SchemaField> entry : fieldMap.entrySet()) {
            fieldMapperStrategy.put(entry.getKey(), entry.getValue().getType().getFieldMapper().get());
        }
        return ImmutableMap.copyOf(fieldMapperStrategy);
    }
}
