package com.jigyasa.dp.search.text;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.SchemaField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

import java.util.Map;

public class PerFieldAnalyzer extends DelegatingAnalyzerWrapper {
    private final Map<String, Analyzer> perFieldAnalyzerMap;
    private final Map<String, SchemaField> schemaFieldMap;

    public PerFieldAnalyzer(Map<String, Analyzer> perFieldAnalyzerMap, Map<String, SchemaField> fieldMap) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.perFieldAnalyzerMap = Map.copyOf(perFieldAnalyzerMap);
        this.schemaFieldMap = Map.copyOf(fieldMap);
    }

    @Override
    public Analyzer getWrappedAnalyzer(String fieldName) {
        return perFieldAnalyzerMap.get(fieldName);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        SchemaField schemaField = this.schemaFieldMap.get(fieldName);
        if (schemaField.getType() == FieldDataType.STRING_COLLECTION) {
            return 100;
        }
        return super.getPositionIncrementGap(fieldName);
    }
}
