package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.entrypoint.IndexManager;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.CompoundFilter;
import com.jigyasa.dp.search.protocol.CountRequest;
import com.jigyasa.dp.search.protocol.CountResponse;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.TermFilter;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Dedicated tests for the Count API.
 */
class CountApiTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private CollectionRegistry registry;
    private IndexSearcherManagerISCH searcherManager;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    @Test
    @DisplayName("Count matchAll with no filters returns total doc count")
    void count_matchAll_noFilters() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder().build());
        assertThat(resp.getCount()).isEqualTo(5);
    }

    @Test
    @DisplayName("Count with term filter")
    void count_withTermFilter() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder()
                .addFilters(termFilter("category", "tech"))
                .build());
        assertThat(resp.getCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("Count with range filter")
    void count_withRangeFilter() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("quantity")
                        .setRangeFilter(RangeFilter.newBuilder()
                                .setMin("50").setMax("200")
                                .setMinExclusive(false).setMaxExclusive(false)))
                .build());
        // quantity: 100(doc1), 50(doc2), 200(doc3), 25(doc4), 75(doc5) → 50,75,100,200 = 4
        assertThat(resp.getCount()).isEqualTo(4);
    }

    @Test
    @DisplayName("Count with compound OR filter")
    void count_withCompoundFilter() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setCompoundFilter(CompoundFilter.newBuilder()
                                .addShould(termFilter("category", "tech"))
                                .addShould(termFilter("category", "food"))))
                .build());
        // All 5 docs are either tech or food
        assertThat(resp.getCount()).isEqualTo(5);
    }

    @Test
    @DisplayName("Count with tenant_id")
    void count_withTenantId() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder()
                .setTenantId("tenantA")
                .build());
        assertThat(resp.getCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("Count on empty index returns 0")
    void count_emptyIndex() throws Exception {
        // No docs indexed
        writer.commit();
        setupMocks();

        CountResponse resp = executeCount(CountRequest.newBuilder().build());
        assertThat(resp.getCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Count after delete reflects deletion")
    void count_afterDelete() throws Exception {
        indexTestData();
        writer.commit();
        setupMocks();

        // Verify initial count
        CountResponse before = executeCount(CountRequest.newBuilder().build());
        assertThat(before.getCount()).isEqualTo(5);

        // Delete doc1 by key
        writer.deleteDocuments(new org.apache.lucene.index.Term("id", "doc1"));
        writer.commit();

        // Need new searcher after delete
        setupMocks();
        CountResponse after = executeCount(CountRequest.newBuilder().build());
        assertThat(after.getCount()).isEqualTo(4);
    }

    // --- Helpers ---

    private void setupMocks() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(searcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManager.SearcherLease(searcher, searcherManager));

        HandlerHelpers helpers = mock(HandlerHelpers.class);
        when(helpers.indexSchemaManager()).thenReturn(schemaManager);
        when(helpers.indexSearcherManager()).thenReturn(searcherManager);

        registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
    }

    private CountResponse executeCount(CountRequest req) {
        IndexManager indexManager = new IndexManager(registry);
        AtomicReference<CountResponse> result = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        StreamObserver<CountResponse> observer = mock(StreamObserver.class);
        doAnswer(inv -> {
            result.set(inv.getArgument(0));
            return null;
        }).when(observer).onNext(any());

        indexManager.count(req, observer);
        verify(observer, never()).onError(any());
        assertThat(result.get()).isNotNull();
        return result.get();
    }

    private void indexDoc(String id, String title, String category,
                          int quantity, String tenantId) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"category\":\"%s\",\"quantity\":%d}",
                id, title, category, quantity);

        IndexItem.Builder itemBuilder = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json);
        if (tenantId != null && !tenantId.isEmpty()) {
            itemBuilder.setTenantId(tenantId);
        }
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(itemBuilder.build())
                .build();
        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "Machine Learning", "tech", 100, "tenantA");
        indexDoc("doc2", "Database Internals", "tech", 50, "tenantA");
        indexDoc("doc3", "Cooking Easy", "food", 200, "tenantA");
        indexDoc("doc4", "Advanced AI", "tech", 25, "tenantB");
        indexDoc("doc5", "Food Science", "food", 75, "tenantB");
    }

    private static FilterClause termFilter(String field, String value) {
        return FilterClause.newBuilder()
                .setField(field)
                .setTermFilter(TermFilter.newBuilder().setValue(value))
                .build();
    }

    private IndexSchema buildSchema() {
        SchemaField id = new SchemaField();
        id.setName("id");
        id.setType(FieldDataType.STRING);
        id.setKey(true);
        id.setFilterable(true);

        SchemaField title = new SchemaField();
        title.setName("title");
        title.setType(FieldDataType.STRING);
        title.setSearchable(true);

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);

        SchemaField quantity = new SchemaField();
        quantity.setName("quantity");
        quantity.setType(FieldDataType.INT32);
        quantity.setFilterable(true);
        quantity.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, category, quantity});
        schema.setBm25Config(new BM25Config());
        schema.setTtlEnabled(true);
        return schema;
    }
}
