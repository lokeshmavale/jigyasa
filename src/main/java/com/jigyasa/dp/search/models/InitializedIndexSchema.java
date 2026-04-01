package com.jigyasa.dp.search.models;

import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import lombok.Data;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similarities.Similarity;

import java.util.Map;

@Data
public class InitializedIndexSchema {
    private Map<String, SchemaField> fieldLookupMap;
    private ThreadLocal<Map<String, FieldMapperStrategy>> fieldMapperStrategyMap;
    private SchemaField keyField;
    private Analyzer indexAnalyzer;
    private Analyzer searchAnalyzer;
    private Similarity bm25Similarity;

    public String getKeyFieldName() {
        return keyField.getName();
    }
}
