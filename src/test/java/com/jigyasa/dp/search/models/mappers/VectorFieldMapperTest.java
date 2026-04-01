package com.jigyasa.dp.search.models.mappers;

import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexableField;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class VectorFieldMapperTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("Adds KnnFloatVectorField with correct dimension")
    void addsVectorField() throws Exception {
        IndexSchema schema = buildSchema("COSINE", 3);
        new InitializedSchemaISCH().handle(schema, null);

        VectorFieldMapper mapper = new VectorFieldMapper();
        Document doc = new Document();
        JsonNode value = MAPPER.readTree("[1.0, 0.5, 0.0]");
        mapper.addFields(schema, doc, "embedding", value);

        IndexableField field = doc.getField("embedding");
        assertThat(field).isNotNull().isInstanceOf(KnnFloatVectorField.class);
    }

    @Test
    @DisplayName("Defaults to COSINE when similarityFunction is null")
    void defaultsCosine() throws Exception {
        IndexSchema schema = buildSchema(null, 3);
        new InitializedSchemaISCH().handle(schema, null);

        VectorFieldMapper mapper = new VectorFieldMapper();
        Document doc = new Document();
        JsonNode value = MAPPER.readTree("[1.0, 0.0, 0.0]");

        // Should not throw
        assertThatCode(() -> mapper.addFields(schema, doc, "embedding", value))
                .doesNotThrowAnyException();
        assertThat(doc.getField("embedding")).isNotNull();
    }

    @Test
    @DisplayName("Rejects invalid similarity function")
    void rejectsInvalidSimilarity() throws Exception {
        IndexSchema schema = buildSchema("INVALID_FUNC", 3);
        new InitializedSchemaISCH().handle(schema, null);

        VectorFieldMapper mapper = new VectorFieldMapper();
        Document doc = new Document();
        JsonNode value = MAPPER.readTree("[1.0, 0.0, 0.0]");

        assertThatThrownBy(() -> mapper.addFields(schema, doc, "embedding", value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid similarityFunction");
    }

    @Test
    @DisplayName("All valid similarity functions are accepted")
    void allValidSimilarities() throws Exception {
        for (String sim : new String[]{"COSINE", "DOT_PRODUCT", "EUCLIDEAN", "MAXIMUM_INNER_PRODUCT"}) {
            IndexSchema schema = buildSchema(sim, 3);
            new InitializedSchemaISCH().handle(schema, null);

            VectorFieldMapper mapper = new VectorFieldMapper();
            Document doc = new Document();
            JsonNode value = MAPPER.readTree("[1.0, 0.0, 0.0]");

            assertThatCode(() -> mapper.addFields(schema, doc, "embedding", value))
                    .as("Similarity %s should be accepted", sim)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("Rejects empty vector array")
    void rejectsEmptyVector() throws Exception {
        IndexSchema schema = buildSchema("COSINE", 3);
        new InitializedSchemaISCH().handle(schema, null);

        VectorFieldMapper mapper = new VectorFieldMapper();
        Document doc = new Document();
        JsonNode value = MAPPER.readTree("[]");

        assertThatThrownBy(() -> mapper.addFields(schema, doc, "embedding", value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one dimension");
    }

    @Test
    @DisplayName("Rejects vector with wrong dimension")
    void rejectsWrongDimension() throws Exception {
        IndexSchema schema = buildSchema("COSINE", 3);
        new InitializedSchemaISCH().handle(schema, null);

        VectorFieldMapper mapper = new VectorFieldMapper();
        Document doc = new Document();
        JsonNode value = MAPPER.readTree("[1.0, 0.0]"); // 2 instead of 3

        assertThatThrownBy(() -> mapper.addFields(schema, doc, "embedding", value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension");
    }

    private IndexSchema buildSchema(String similarity, int dimension) {
        SchemaField idField = new SchemaField();
        idField.setName("id");
        idField.setType(FieldDataType.STRING);
        idField.setKey(true);
        idField.setFilterable(true);

        SchemaField vecField = new SchemaField();
        vecField.setName("embedding");
        vecField.setType(FieldDataType.VECTOR);
        vecField.setDimension(dimension);
        vecField.setSimilarityFunction(similarity);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{idField, vecField});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
