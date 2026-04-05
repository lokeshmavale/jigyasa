package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;

import java.util.function.Supplier;

/**
 * Generic collection field mapper that iterates array elements
 * and delegates each to a scalar field mapper.
 * Replaces 6 identical *CollectionFieldMapper classes.
 */
public class CollectionFieldMapper extends FieldMapperStrategy {
    private final FieldMapperStrategy elementMapper;

    public CollectionFieldMapper(Supplier<? extends FieldMapperStrategy> mapperSupplier) {
        this.elementMapper = mapperSupplier.get();
    }

    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        for (int i = 0; i < value.size(); i++) {
            elementMapper.addFields(schema, doc, fieldName, value.get(i));
        }
    }
}
