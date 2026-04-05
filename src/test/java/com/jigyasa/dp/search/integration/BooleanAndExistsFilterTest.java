package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.handlers.QueryRequestHandler;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.CompoundFilter;
import com.jigyasa.dp.search.protocol.ExistsFilter;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
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
 * Tests for CompoundFilter (OR/NOT boolean composition) and ExistsFilter.
 */
class BooleanAndExistsFilterTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);

        indexTestData();
        writer.commit();
        writer.forceMerge(1);
        writer.commit();

        setupHandler();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    // =====================================================================
    // CompoundFilter (OR / NOT)
    // =====================================================================

    @Test
    @DisplayName("OR filter matches documents in either clause")
    void orFilter_matchesEither() {
        FilterClause orFilter = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addShould(termFilter("category", "tech"))
                        .addShould(termFilter("category", "food")))
                .build();

        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(orFilter)
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(5);
    }

    @Test
    @DisplayName("NOT filter excludes matching documents")
    void notFilter_excludes() {
        FilterClause notFilter = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addMustNot(termFilter("category", "tech")))
                .build();

        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(notFilter)
                .setTopK(10)
                .build());

        // doc3 (food), doc5 (food) remain
        assertThat(resp.getTotalHits()).isEqualTo(2);
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsExactlyInAnyOrder("doc3", "doc5");
    }

    @Test
    @DisplayName("Combined must and must_not filters work together")
    void orAndNotCombined() {
        FilterClause compound = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addMust(termFilter("active", "T"))
                        .addMustNot(termFilter("category", "food")))
                .build();

        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(compound)
                .setTopK(10)
                .build());

        // active=true: doc1,doc2,doc4,doc5; minus food(doc5) = doc1,doc2,doc4
        assertThat(resp.getTotalHits()).isEqualTo(3);
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsExactlyInAnyOrder("doc1", "doc2", "doc4");
    }

    @Test
    @DisplayName("Nested CompoundFilter works")
    void nestedCompound() {
        // (category=tech OR category=food) AND active=true
        FilterClause inner = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addShould(termFilter("category", "tech"))
                        .addShould(termFilter("category", "food")))
                .build();

        FilterClause outer = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addMust(inner)
                        .addMust(termFilter("active", "T")))
                .build();

        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(outer)
                .setTopK(10)
                .build());

        // All 5 match OR, minus active=false (doc3) = 4
        assertThat(resp.getTotalHits()).isEqualTo(4);
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsExactlyInAnyOrder("doc1", "doc2", "doc4", "doc5");
    }

    // =====================================================================
    // ExistsFilter
    // =====================================================================

    @Test
    @DisplayName("ExistsFilter with must_exist=true returns docs with field")
    void existsFilter_fieldExists() {
        // All 5 docs have price, so all should match
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(existsFilter("price", true))
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(5);
    }

    @Test
    @DisplayName("ExistsFilter with must_exist=false returns docs missing field")
    void existsFilter_fieldMissing() {
        // All 5 docs have price, so must_exist=false should return 0
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(existsFilter("price", false))
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(0);
    }

    @Test
    @DisplayName("ExistsFilter combined with term filter")
    void existsFilter_withOtherFilters() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .addFilters(existsFilter("price", true))
                .addFilters(termFilter("category", "tech"))
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(3);
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsExactlyInAnyOrder("doc1", "doc2", "doc4");
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private QueryResponse executeQuery(QueryRequest req) {
        AtomicReference<QueryResponse> result = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        doAnswer(inv -> {
            result.set(inv.getArgument(0));
            return null;
        }).when(observer).onNext(any());

        handler.internalHandle(req, observer);
        verify(observer, never()).onError(any());
        assertThat(result.get()).isNotNull();
        return result.get();
    }

    private void indexDoc(String id, String title, String category,
                          boolean active, double price) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"category\":\"%s\"," +
                        "\"active\":%s,\"price\":%s}",
                id, title, category, active, price);

        IndexItem item = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json)
                .build();
        IndexRequest req = IndexRequest.newBuilder().addItem(item).build();
        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "Machine Learning Basics", "tech", true, 29.99);
        indexDoc("doc2", "Database Internals", "tech", true, 49.99);
        indexDoc("doc3", "Cooking Made Easy", "food", false, 15.99);
        indexDoc("doc4", "Advanced AI Systems", "tech", true, 79.99);
        indexDoc("doc5", "Food Science", "food", true, 35.99);
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        IndexSearcherManagerISCH searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManagerISCH.SearcherLease(acquiredSearcher, searcherManager));

        HandlerHelpers helpers = mock(HandlerHelpers.class);
        when(helpers.getIndexSchemaManager()).thenReturn(schemaManager);
        when(helpers.getIndexSearcherManager()).thenReturn(searcherManager);

        CollectionRegistry registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        handler = new QueryRequestHandler(registry);
    }

    private static FilterClause termFilter(String field, String value) {
        return FilterClause.newBuilder()
                .setField(field)
                .setTermFilter(TermFilter.newBuilder().setValue(value))
                .build();
    }

    private static FilterClause existsFilter(String field, boolean mustExist) {
        return FilterClause.newBuilder()
                .setField(field)
                .setExistsFilter(ExistsFilter.newBuilder().setMustExist(mustExist))
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
        title.setFilterable(true);
        title.setSortable(true);

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);
        category.setSortable(true);

        SchemaField active = new SchemaField();
        active.setName("active");
        active.setType(FieldDataType.BOOLEAN);
        active.setFilterable(true);

        SchemaField price = new SchemaField();
        price.setName("price");
        price.setType(FieldDataType.DOUBLE);
        price.setFilterable(true);
        price.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, category, active, price});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
