package com.jigyasa.dp.search.models.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import org.apache.lucene.document.Document;

public class BooleanFieldMapper extends StringFieldMapper {

    public static String getStringValue(boolean val) {
        return val ? "T" : "F";
    }

    public static boolean getBoolValue(String val) {
        return "T".equals(val);
    }

    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);
        String stringValue = getStringValue(value.asBoolean());
        addStringToDoc(doc, schemaField, stringValue);
    }
}
