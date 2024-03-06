package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;

public class Int64CollectionFieldMapper extends FieldMapperStrategy {
    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        for (int i = 0; i < value.size(); i++) {
            FieldMapperStrategy strategy = new Int64FieldMapper();
            strategy.addFields(schema, doc, fieldName, value.get(i));
        }
    }
}
