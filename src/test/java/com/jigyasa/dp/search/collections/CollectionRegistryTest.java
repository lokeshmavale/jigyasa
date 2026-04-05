package com.jigyasa.dp.search.collections;

import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.models.ServerMode;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CollectionRegistryTest {

    private Path tempIndexDir;
    private Path tempTranslogDir;
    private CollectionRegistry registry;

    @BeforeEach
    void setUp() throws IOException {
        tempIndexDir = Files.createTempDirectory("jigyasa-idx-");
        tempTranslogDir = Files.createTempDirectory("jigyasa-tlog-");
        registry = new CollectionRegistry(
                tempIndexDir.toString(),
                tempTranslogDir.toString(),
                ServerMode.READ_WRITE);
        registry.initialize(buildSchema());
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.shutdownAll();
        }
        deleteQuietly(tempIndexDir);
        deleteQuietly(tempTranslogDir);
    }

    private static void deleteQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try { Files.deleteIfExists(file); } catch (IOException ignored) {}
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc) {
                    try { Files.deleteIfExists(d); } catch (IOException ignored) {}
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {}
    }

    @Test
    void createCollection_andVerifyExists() {
        registry.createCollection("my-collection", buildSchema());

        assertThat(registry.exists("my-collection")).isTrue();
        assertThat(registry.resolveHelpers("my-collection")).isNotNull();
    }

    @Test
    void createCollection_invalidName_throws() {
        assertThatThrownBy(() -> registry.createCollection("../evil", buildSchema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid collection name");

        assertThatThrownBy(() -> registry.createCollection("a/b", buildSchema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid collection name");

        assertThatThrownBy(() -> registry.createCollection("", buildSchema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already exists"); // empty resolves to "default" which exists
    }

    @Test
    void createDuplicateCollection_throws() {
        registry.createCollection("dup-test", buildSchema());

        assertThatThrownBy(() -> registry.createCollection("dup-test", buildSchema()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void closeCollection_thenRequestsFail() {
        registry.createCollection("closeable", buildSchema());
        assertThat(registry.exists("closeable")).isTrue();

        registry.closeCollection("closeable");

        assertThat(registry.exists("closeable")).isFalse();
        assertThatThrownBy(() -> registry.resolveHelpers("closeable"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void closeDefaultCollection_succeeds() {
        // The registry does not prevent closing the default collection
        registry.closeCollection(CollectionRegistry.DEFAULT_COLLECTION);
        assertThat(registry.exists(CollectionRegistry.DEFAULT_COLLECTION)).isFalse();
    }

    @Test
    void openPreviouslyClosedCollection_dataPreserved() throws Exception {
        String name = "reopen-test";
        registry.createCollection(name, buildSchema());

        // Write a document so there's data on disk
        HandlerHelpers helpers = registry.resolveHelpers(name);
        IndexWriter writer = helpers.indexWriterManager().acquireWriter();
        try {
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument("{\"id\":\"1\",\"content\":\"hello world\"}")
                            .build())
                    .build();
            IndexSchema schema = helpers.indexSchemaManager().getIndexSchema();
            IndexRequestHandler.processIndexRequests(req, schema, writer, null);
            writer.commit();
        } finally {
            helpers.indexWriterManager().releaseWriter();
        }

        registry.closeCollection(name);
        assertThat(registry.exists(name)).isFalse();

        // Reopen with same schema
        registry.openCollection(name, buildSchema());
        assertThat(registry.exists(name)).isTrue();

        // Verify data is preserved by searching
        HandlerHelpers reopenedHelpers = registry.resolveHelpers(name);
        IndexSearcher searcher = reopenedHelpers.indexSearcherManager().acquireSearcher();
        try {
            int totalHits = searcher.count(new MatchAllDocsQuery());
            assertThat(totalHits).isEqualTo(1);
        } finally {
            reopenedHelpers.indexSearcherManager().releaseSearcher(searcher);
        }
    }

    @Test
    void listCollections_returnsCorrectNames() {
        // Default collection is created in setUp
        Collection<String> initial = registry.listCollections();
        assertThat(initial).containsExactly("default");

        registry.createCollection("alpha", buildSchema());
        registry.createCollection("beta", buildSchema());

        Collection<String> afterCreate = registry.listCollections();
        assertThat(afterCreate).containsExactlyInAnyOrder("default", "alpha", "beta");

        registry.closeCollection("alpha");
        Collection<String> afterClose = registry.listCollections();
        assertThat(afterClose).containsExactlyInAnyOrder("default", "beta");
    }

    @Test
    void schemaPersistence_reopenWithoutSchema_readsFromIndex() throws Exception {
        String name = "schema-persist";
        registry.createCollection(name, buildSchema());

        // Write a document to trigger schema persistence via commit
        HandlerHelpers helpers = registry.resolveHelpers(name);
        IndexWriter writer = helpers.indexWriterManager().acquireWriter();
        try {
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument("{\"id\":\"doc1\",\"content\":\"test content\"}")
                            .build())
                    .build();
            IndexSchema schema = helpers.indexSchemaManager().getIndexSchema();
            IndexRequestHandler.processIndexRequests(req, schema, writer, null);
            writer.commit();
        } finally {
            helpers.indexWriterManager().releaseWriter();
        }

        registry.closeCollection(name);

        // Reopen with null schema— should read persisted schema from index commit data
        registry.openCollection(name, null);
        assertThat(registry.exists(name)).isTrue();

        // Verify collection is usable: acquire searcher and search
        HandlerHelpers reopenedHelpers = registry.resolveHelpers(name);
        IndexSearcher searcher = reopenedHelpers.indexSearcherManager().acquireSearcher();
        try {
            int totalHits = searcher.count(new MatchAllDocsQuery());
            assertThat(totalHits).isEqualTo(1);
        } finally {
            reopenedHelpers.indexSearcherManager().releaseSearcher(searcher);
        }
    }

    private IndexSchema buildSchema() {
        SchemaField idField = new SchemaField();
        idField.setName("id");
        idField.setType(FieldDataType.STRING);
        idField.setKey(true);
        idField.setFilterable(true);
        idField.setSearchable(false);
        idField.setSortable(false);

        SchemaField contentField = new SchemaField();
        contentField.setName("content");
        contentField.setType(FieldDataType.STRING);
        contentField.setKey(false);
        contentField.setFilterable(false);
        contentField.setSearchable(true);
        contentField.setSortable(false);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{idField, contentField});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
