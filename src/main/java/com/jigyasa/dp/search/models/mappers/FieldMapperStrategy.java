package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.*;
import org.apache.lucene.util.BytesRef;

import java.nio.charset.StandardCharsets;

public abstract class FieldMapperStrategy {
    private static final BytesRef EMPTY_BREF = new BytesRef();
    public static final String SOURCE_FEILD_NAME = "_src";

    public abstract void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value);

    protected static Field getDocValuesField(String fieldName, FieldDataType dataType) {
        return switch (dataType) {
            case STRING, BOOLEAN ->
                    new SortedDocValuesField(fieldName, EMPTY_BREF);
            case STRING_COLLECTION, BOOLEAN_COLLECTION ->
                    new SortedSetDocValuesField(fieldName, EMPTY_BREF);
            case INT32, INT64, DATE_TIME_OFFSET -> new NumericDocValuesField(fieldName, 0);
            case DOUBLE -> new DoubleDocValuesField(fieldName, 0.0);
            case INT32_COLLECTION, INT64_COLLECTION, DATE_TIME_OFFSET_COLLECTION, DOUBLE_COLLECTION ->
                    new SortedNumericDocValuesField(fieldName, 0);
            case GEO_POINT -> new LatLonDocValuesField(fieldName, 0.0, 0.0);
            default -> throw new IllegalArgumentException("Unsupported DocValuesField " + dataType.name());
        };
    }

    public static void addSourceField(Document luceneDoc, String document) {
        luceneDoc.add(new StoredField(SOURCE_FEILD_NAME, document.getBytes(StandardCharsets.UTF_8)));
    }
}
