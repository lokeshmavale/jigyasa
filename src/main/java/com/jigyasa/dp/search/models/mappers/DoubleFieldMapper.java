package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;

import java.util.HashMap;
import java.util.Map;

public class DoubleFieldMapper extends FieldMapperStrategy {
    private final Map<String, Field> fieldMap = new HashMap<>();

    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);
        double val = value.asDouble();

        if (schemaField.isFilterable()) {
            doc.add(getStoredField(schemaField, val));
            if (!FieldDataType.isCollection(schemaField.getType())) {
                doc.add(getFilterableDocField(schemaField, val));
            }
        }

        if (schemaField.isSortable()) {
            doc.add(getSortableField(schemaField, val));
        }
    }

    private IndexableField getSortableField(SchemaField schemaField, double val) {
        String fieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, k -> getDocValuesField(k, schemaField.getType()));
        field.setDoubleValue(val);
        return field;
    }

    private IndexableField getFilterableDocField(SchemaField schemaField, double val) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        Field docValuesField = getDocValuesField(fieldName, schemaField.getType());
        docValuesField.setDoubleValue(val);
        return docValuesField;
    }

    private IndexableField getStoredField(SchemaField schemaField, double val) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, k -> new DoublePoint(fieldName));
        field.setDoubleValue(val);
        return field;
    }
}
