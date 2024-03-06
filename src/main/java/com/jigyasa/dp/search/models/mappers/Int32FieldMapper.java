package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexableField;

import java.util.HashMap;
import java.util.Map;

public class Int32FieldMapper extends FieldMapperStrategy {
    private final Map<String, Field> fieldMap = new HashMap<>();

    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);
        int val = value.asInt();

        if (schemaField.isFilterable()) {
            doc.add(getFilterableField(schemaField, val));
            if (!FieldDataType.isCollection(schemaField.getType())) {
                doc.add(getFilterableDocField(schemaField, val));
            }
        }

        if (schemaField.isSortable()) {
            doc.add(getSortableField(schemaField, val));
        }
    }

    private IndexableField getSortableField(SchemaField schemaField, int val) {
        String fieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, k -> getDocValuesField(k, schemaField.getType()));
        field.setIntValue(val);
        return field;
    }

    private IndexableField getFilterableDocField(SchemaField schemaField, int val) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        Field docValuesField = getDocValuesField(fieldName, schemaField.getType());
        docValuesField.setIntValue(val);
        return docValuesField;
    }

    private IndexableField getFilterableField(SchemaField schemaField, int val) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, IntPoint::new);
        field.setIntValue(val);
        return field;
    }
}
