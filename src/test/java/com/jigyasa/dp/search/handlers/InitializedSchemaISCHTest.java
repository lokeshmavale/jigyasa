package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.models.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class InitializedSchemaISCHTest {

    private final InitializedSchemaISCH handler = new InitializedSchemaISCH();

    @Test
    @DisplayName("Initializes schema with valid fields")
    void initializesValidSchema() {
        IndexSchema schema = buildSchema();
        handler.handle(schema, null);

        InitializedIndexSchema init = schema.getInitializedSchema();
        assertThat(init).isNotNull();
        assertThat(init.getKeyField().getName()).isEqualTo("id");
        assertThat(init.getFieldLookupMap()).containsKeys("id", "title", "score");
        assertThat(init.getIndexAnalyzer()).isNotNull();
        assertThat(init.getSearchAnalyzer()).isNotNull();
        assertThat(init.getBm25Similarity()).isNotNull();
    }

    @Test
    @DisplayName("Rejects schema with no key field")
    void rejectsNoKeyField() {
        SchemaField field = new SchemaField();
        field.setName("title");
        field.setType(FieldDataType.STRING);
        field.setSearchable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{field});
        schema.setBm25Config(new BM25Config());

        assertThatThrownBy(() -> handler.handle(schema, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key field");
    }

    @Test
    @DisplayName("Rejects schema with multiple key fields")
    void rejectsMultipleKeyFields() {
        SchemaField f1 = new SchemaField();
        f1.setName("id1");
        f1.setType(FieldDataType.STRING);
        f1.setKey(true);
        f1.setFilterable(true);

        SchemaField f2 = new SchemaField();
        f2.setName("id2");
        f2.setType(FieldDataType.STRING);
        f2.setKey(true);
        f2.setFilterable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{f1, f2});
        schema.setBm25Config(new BM25Config());

        assertThatThrownBy(() -> handler.handle(schema, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key field");
    }

    @Test
    @DisplayName("FieldMapperStrategy map is per-thread (ThreadLocal)")
    void fieldMapperIsThreadLocal() throws Exception {
        IndexSchema schema = buildSchema();
        handler.handle(schema, null);

        var map1 = schema.getInitializedSchema().getFieldMapperStrategyMap().get();
        var map2 = new Object[1];

        Thread t = new Thread(() -> map2[0] = schema.getInitializedSchema().getFieldMapperStrategyMap().get());
        t.start();
        t.join();

        // Different thread should get different mapper instances
        assertThat(map1).isNotSameAs(map2[0]);
    }

    @Test
    @DisplayName("BM25 config defaults applied when null")
    void bm25Defaults() {
        IndexSchema schema = buildSchema();
        schema.setBm25Config(new BM25Config()); // uses defaults

        handler.handle(schema, null);

        assertThat(schema.getInitializedSchema().getBm25Similarity()).isNotNull();
    }

    private IndexSchema buildSchema() {
        SchemaField idField = new SchemaField();
        idField.setName("id");
        idField.setType(FieldDataType.STRING);
        idField.setKey(true);
        idField.setFilterable(true);

        SchemaField titleField = new SchemaField();
        titleField.setName("title");
        titleField.setType(FieldDataType.STRING);
        titleField.setSearchable(true);
        titleField.setFilterable(true);
        titleField.setSortable(true);

        SchemaField scoreField = new SchemaField();
        scoreField.setName("score");
        scoreField.setType(FieldDataType.DOUBLE);
        scoreField.setFilterable(true);
        scoreField.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{idField, titleField, scoreField});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
