package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;

public class VectorFieldMapper extends FieldMapperStrategy {

    private static final VectorSimilarityFunction DEFAULT_SIMILARITY = VectorSimilarityFunction.COSINE;

    @Override
    public void addFields(IndexSchema schema, Document doc, String fieldName, JsonNode value) {
        SchemaField schemaField = schema.getInitializedSchema().getFieldLookupMap().get(fieldName);

        if (value.size() == 0) {
            throw new IllegalArgumentException("Vector field '" + fieldName + "' must have at least one dimension");
        }
        if (schemaField.getDimension() != null && value.size() != schemaField.getDimension()) {
            throw new IllegalArgumentException("Vector field '" + fieldName + "' expects dimension "
                    + schemaField.getDimension() + " but got " + value.size());
        }

        float[] vals = new float[value.size()];
        for (int i = 0; i < value.size(); i++) {
            vals[i] = (float) value.get(i).asDouble();
        }

        VectorSimilarityFunction similarity = resolveSimilarity(schemaField.getSimilarityFunction());
        doc.add(new KnnFloatVectorField(schemaField.getName(), vals, similarity));
    }

    private VectorSimilarityFunction resolveSimilarity(String name) {
        if (StringUtils.isEmpty(name)) {
            return DEFAULT_SIMILARITY;
        }
        try {
            return VectorSimilarityFunction.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid similarityFunction: '" + name + "'. Valid values: COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT");
        }
    }
}
