package com.jigyasa.dp.search.codecs;

import com.jigyasa.dp.search.configs.EnvironmentVariables;
import com.jigyasa.dp.search.models.HNSWConfig;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

@RequiredArgsConstructor
public class CustomLucene99AnweshanHNSWVectorsFormat extends PerFieldKnnVectorsFormat {
    private static int MAX_DIMENSION = Integer.parseInt(EnvironmentVariables.MAX_VECTOR_DIMENSION.defaultIfEmpty());
    private final InitializedIndexSchema schema;


    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        SchemaField schemaField = schema.getFieldLookupMap().get(field);
        HNSWConfig config = schema.getHNSWConfigMap().get(schemaField.getHNSWConfigName());
        //Todo: Later explore possibility of optimization in parallel writers in overloaded ctor for below
        return new Lucene99HnswVectorsFormat(config.getM(), config.getEfConstruction());
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMENSION;
    }
}
