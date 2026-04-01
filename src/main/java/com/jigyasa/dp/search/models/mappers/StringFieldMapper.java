package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import java.util.HashMap;
import java.util.Map;

public class StringFieldMapper extends FieldMapperStrategy {

    private final Map<String, Field> fieldMap = new HashMap<>();

    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);
        String val = value.asText();

        addStringToDoc(doc, schemaField, val);
    }

    protected void addStringToDoc(Document doc, SchemaField schemaField, String val) {
        if (schemaField.isKey()) {
            // Key field needs raw name indexed for updateDocument/deleteDocuments Term matching
            doc.add(getFilterableField(schemaField, val));
            Field keyField = new StringField(schemaField.getName(), val, Field.Store.NO);
            doc.add(keyField);
        } else if (schemaField.isFilterable()) {
            doc.add(getFilterableField(schemaField, val));
        }

        if (schemaField.isSearchable()) {
            doc.add(getSearchableField(schemaField, val));
        }

        if (schemaField.isSortable()) {
            doc.add(getStringDocValuesField(schemaField, val));
        }
    }

    private IndexableField getSearchableField(SchemaField schemaField, String val) {
        String fieldName = schemaField.getName();
        Field field = fieldMap.computeIfAbsent(fieldName, k -> new TextField(k, "", Field.Store.NO));
        field.setStringValue(val);
        return field;
    }

    public IndexableField getFilterableField(SchemaField schemaField, String val) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, k -> new StringField(k, "", Field.Store.NO));
        field.setStringValue(val);
        return field;
    }

    public IndexableField getStringDocValuesField(SchemaField schemaField, String val) {
        String fieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(schemaField.getName());
        Field field = fieldMap.computeIfAbsent(fieldName, k -> getDocValuesField(k, schemaField.getType()));
        field.setBytesValue(new BytesRef(val));
        return field;
    }


}
