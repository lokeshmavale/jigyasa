package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.IndexableField;

import java.util.HashMap;
import java.util.Map;

public class GeoPointFieldMapper extends FieldMapperStrategy {

    private final Map<String, Field> fieldMap = new HashMap<>();

    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);
        if (value == null || !value.has("lat") || !value.has("lon")) {
            throw new IllegalArgumentException("GEO_POINT field '" + fieldName + "' requires object with 'lat' and 'lon' keys");
        }
        double lat = value.get("lat").asDouble();
        double lon = value.get("lon").asDouble();

        if (schemaField.isFilterable()) {
            doc.add(getFilterableField(schemaField, lat, lon));
            if (!FieldDataType.isCollection(schemaField.getType())) {
                doc.add(getFilterableDocField(schemaField, lat, lon));
            }
        }

        if (schemaField.isSortable()) {
            doc.add(getSortableField(schemaField, lat, lon));
        }

    }

    private IndexableField getSortableField(SchemaField schemaField, double lat, double lon) {
        String fieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(schemaField.getName());
        LatLonDocValuesField field = (LatLonDocValuesField) fieldMap.computeIfAbsent(fieldName, k -> getDocValuesField(k, schemaField.getType()));
        field.setLocationValue(lat, lon);
        return field;
    }

    private IndexableField getFilterableDocField(SchemaField schemaField, double lat, double lon) {
        return new LatLonDocValuesField(LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName()), lat, lon);
    }

    private IndexableField getFilterableField(SchemaField schemaField, double lat, double lon) {
        String fieldName = LuceneFieldType.FILTERABLE.toLuceneFieldName(schemaField.getName());
        LatLonPoint field = (LatLonPoint) fieldMap.computeIfAbsent(fieldName, k -> new LatLonPoint(k, 0.0F, 0.0));
        field.setLocationValue(lat, lon);
        return field;
    }
}
