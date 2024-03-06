package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.HNSWConfig;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;

import java.util.Objects;

public class VectorFieldMapper extends FieldMapperStrategy {
    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);

        if (StringUtils.isEmpty(schemaField.getHNSWConfigName())) {
            throw new IllegalArgumentException("Must specify HNSWConfigName for vector field " + fieldName);
        }

        float[] vals = new float[value.size()];

        for (int i = 0; i < value.size(); i++) {
            vals[i] = (float) value.get(i).asDouble();
        }

        HNSWConfig config = schema.getInitializedSchema().getHNSWConfigMap().get(schemaField.getHNSWConfigName());

        Objects.requireNonNull(config, "No HNSWConfig found with name " + schemaField.getHNSWConfigName());

        KnnFloatVectorField field = new KnnFloatVectorField(schemaField.getName(), vals, config.getDistanceMetric());
        doc.add(field);
    }
}
