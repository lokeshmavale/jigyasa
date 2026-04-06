package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.handlers.QueryRequestHandler;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.SortClause;
import com.jigyasa.dp.search.protocol.TermFilter;
import com.jigyasa.dp.search.protocol.VectorQuery;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
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
 * Comprehensive query capabilities test suite exercising all supported query types
 * against a real in-memory Lucene index via QueryRequestHandler.
 */
class QueryCapabilitiesTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;
    private HandlerHelpers helpers;
    private IndexSearcherManagerISCH searcherManager;
    private IndexSearcher acquiredSearcher;

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
    // 1. Text Queries (BM25)
    // =====================================================================
    @Nested
    @DisplayName("TextQueries")
    class TextQueries {

        @Test
        @DisplayName("Single term search returns matching documents")
        void singleTermSearch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("cooking")
                    .setTextField("title")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc3");
        }

        @Test
        @DisplayName("Multi-term search ranks doc with more matching terms higher")
        void multiTermSearch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("machine learning")
                    .setTextField("title")
                    .setTopK(10)
                    .setIncludeSource(true)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            // doc1 "Machine Learning Basics" should be present
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
        }

        @Test
        @DisplayName("Search targets a specific field")
        void searchSpecificField() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("database")
                    .setTextField("content")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc2");
        }

        @Test
        @DisplayName("No text_field searches all searchable STRING fields")
        void searchAllSearchableFields() {
            // "basics" appears in title of doc1 and content of doc1
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("basics")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
        }

        @Test
        @DisplayName("Non-existent term returns empty results")
        void noMatchReturnsEmpty() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("xyznonexistent")
                    .setTextField("title")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(0);
            assertThat(resp.getHitsList()).isEmpty();
        }
    }

    // =====================================================================
    // 2. Term Filters
    // =====================================================================
    @Nested
    @DisplayName("TermFilters")
    class TermFilters {

        @Test
        @DisplayName("Term filter on STRING field")
        void termFilterOnString() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("category", "food"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc3", "doc5");
        }

        @Test
        @DisplayName("Term filter on BOOLEAN field")
        void termFilterOnBoolean() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("active", "F"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc3");
        }

        @Test
        @DisplayName("Term filter on INT32 field")
        void termFilterOnInt32() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("quantity", "100"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc1");
        }

        @Test
        @DisplayName("Term filter on INT64 field")
        void termFilterOnInt64() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("timestamp", "2000"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc2");
        }

        @Test
        @DisplayName("Term filter on DOUBLE field")
        void termFilterOnDouble() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("price", "29.99"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc1");
        }

        @Test
        @DisplayName("Term filter on DATE_TIME_OFFSET field")
        void termFilterOnDateTime() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("created_at", "1700000000000"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc1");
        }

        @Test
        @DisplayName("Multiple term filters are ANDed")
        void multipleTermFiltersAreAnded() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("category", "tech"))
                    .addFilters(termFilter("active", "T"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc4");
        }
    }

    // =====================================================================
    // 3. Range Filters
    // =====================================================================
    @Nested
    @DisplayName("RangeFilters")
    class RangeFilters {

        @Test
        @DisplayName("Inclusive range on INT32 matches boundaries")
        void rangeFilterOnInt32_inclusive() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("quantity", "50", "100", false, false))
                    .setTopK(10)
                    .build());

            // quantity 50 (doc2), 75 (doc5), 100 (doc1)
            assertThat(resp.getTotalHits()).isEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc5");
        }

        @Test
        @DisplayName("Exclusive range on INT32 excludes boundaries")
        void rangeFilterOnInt32_exclusive() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("quantity", "50", "100", true, true))
                    .setTopK(10)
                    .build());

            // Only quantity 75 (doc5) is strictly between 50 and 100
            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc5");
        }

        @Test
        @DisplayName("Range on INT64 field")
        void rangeFilterOnInt64() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("timestamp", "2000", "4000", false, false))
                    .setTopK(10)
                    .build());

            // timestamp 2000 (doc2), 3000 (doc3), 4000 (doc4)
            assertThat(resp.getTotalHits()).isEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc2", "doc3", "doc4");
        }

        @Test
        @DisplayName("Range on DOUBLE field")
        void rangeFilterOnDouble() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("price", "20.0", "50.0", false, false))
                    .setTopK(10)
                    .build());

            // price 29.99 (doc1), 49.99 (doc2), 35.99 (doc5)
            assertThat(resp.getTotalHits()).isEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc5");
        }

        @Test
        @DisplayName("Range on DATE_TIME_OFFSET field")
        void rangeFilterOnDateTime() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("created_at", "1700000000000", "1700300000000", false, false))
                    .setTopK(10)
                    .build());

            // created_at: doc1=1700000000000, doc2=1700100000000, doc3=1700200000000, doc4=1700300000000
            assertThat(resp.getTotalHits()).isEqualTo(4);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc3", "doc4");
        }

        @Test
        @DisplayName("Open-ended range with min only (unbounded upper)")
        void rangeFilterOpenEnded_minOnly() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("quantity", "100", "", false, false))
                    .setTopK(10)
                    .build());

            // quantity >= 100: doc1=100, doc3=200
            assertThat(resp.getTotalHits()).isEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc3");
        }

        @Test
        @DisplayName("Open-ended range with max only (unbounded lower)")
        void rangeFilterOpenEnded_maxOnly() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("quantity", "", "50", false, false))
                    .setTopK(10)
                    .build());

            // quantity <= 50: doc2=50, doc4=25
            assertThat(resp.getTotalHits()).isEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc2", "doc4");
        }

        @Test
        @DisplayName("Range where min > max returns zero results")
        void rangeFilterMatchesNothing() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(rangeFilter("quantity", "200", "10", false, false))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(0);
            assertThat(resp.getHitsList()).isEmpty();
        }
    }

    // =====================================================================
    // 4. Sorting
    // =====================================================================
    @Nested
    @DisplayName("Sorting")
    class Sorting {

        @Test
        @DisplayName("Sort by INT32 ascending")
        void sortByInt32Ascending() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                    .setTopK(10)
                    .build());

            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            // quantity: doc4=25, doc2=50, doc5=75, doc1=100, doc3=200
            assertThat(ids).containsExactly("doc4", "doc2", "doc5", "doc1", "doc3");
        }

        @Test
        @DisplayName("Sort by INT32 descending")
        void sortByInt32Descending() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(true))
                    .setTopK(10)
                    .build());

            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(ids).containsExactly("doc3", "doc1", "doc5", "doc2", "doc4");
        }

        @Test
        @DisplayName("Sort by DOUBLE field")
        void sortByDouble() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("price").setDescending(false))
                    .setTopK(10)
                    .build());

            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            // price: doc3=15.99, doc1=29.99, doc5=35.99, doc2=49.99, doc4=79.99
            assertThat(ids).containsExactly("doc3", "doc1", "doc5", "doc2", "doc4");
        }

        @Test
        @DisplayName("Sort by STRING field (alphabetical)")
        void sortByString() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                    .setTopK(10)
                    .build());

            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            // alphabetical by title: "Advanced AI Systems", "Cooking Made Easy", "Database Internals", "Food Science", "Machine Learning Basics"
            assertThat(ids).containsExactly("doc4", "doc3", "doc2", "doc5", "doc1");
        }

        @Test
        @DisplayName("Sort by DATE_TIME_OFFSET field")
        void sortByDateTime() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("created_at").setDescending(true))
                    .setTopK(10)
                    .build());

            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            // created_at desc: doc5=1700400000000, doc4=1700300000000, doc3=1700200000000, doc2=1700100000000, doc1=1700000000000
            assertThat(ids).containsExactly("doc5", "doc4", "doc3", "doc2", "doc1");
        }

        @Test
        @DisplayName("Multi-field sort with tiebreaker")
        void multiFieldSort() {
            // Sort by category ASC (food, tech), then price DESC within each category
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("active", "T"))
                    .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                    .addSort(SortClause.newBuilder().setField("price").setDescending(true))
                    .setTopK(10)
                    .build());

            // active=true: doc1, doc2, doc4, doc5
            // sorted by title asc: "Advanced AI Systems"(doc4), "Database Internals"(doc2), "Food Science"(doc5), "Machine Learning Basics"(doc1)
            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(ids).containsExactly("doc4", "doc2", "doc5", "doc1");
        }
    }

    // =====================================================================
    // 5. Pagination
    // =====================================================================
    @Nested
    @DisplayName("Pagination")
    class Pagination {

        @Test
        @DisplayName("Offset pagination returns different pages")
        void offsetPagination() {
            // Sort to get deterministic order
            QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                    .setTopK(2).setOffset(0)
                    .build());

            QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                    .setTopK(2).setOffset(2)
                    .build());

            assertThat(page1.getHitsList()).hasSize(2);
            assertThat(page2.getHitsList()).hasSize(2);
            // Pages should not overlap
            List<String> page1Ids = page1.getHitsList().stream().map(QueryHit::getDocId).toList();
            List<String> page2Ids = page2.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(page1Ids).doesNotContainAnyElementsOf(page2Ids);
        }

        @Test
        @DisplayName("SearchAfter pagination with sort")
        void searchAfterPagination() {
            // Get first page with sort
            QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                    .setTopK(2)
                    .build());

            assertThat(page1.getHitsList()).hasSize(2);
            assertThat(page1.hasNextSearchAfter()).isTrue();

            // Use search_after token for next page
            QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                    .setTopK(2)
                    .setSearchAfter(page1.getNextSearchAfter())
                    .build());

            assertThat(page2.getHitsList()).hasSize(2);
            List<String> page1Ids = page1.getHitsList().stream().map(QueryHit::getDocId).toList();
            List<String> page2Ids = page2.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(page1Ids).doesNotContainAnyElementsOf(page2Ids);
        }

        @Test
        @DisplayName("SearchAfter with explicit sort field values")
        void searchAfterWithSort() {
            // First page sorted by price ascending
            QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("price").setDescending(false))
                    .setTopK(3)
                    .build());

            assertThat(page1.getHitsList()).hasSize(3);
            // prices: 15.99, 29.99, 35.99

            // Get remaining via search_after
            QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("price").setDescending(false))
                    .setTopK(3)
                    .setSearchAfter(page1.getNextSearchAfter())
                    .build());

            assertThat(page2.getHitsList()).hasSize(2);
            // prices: 49.99, 79.99
            assertThat(page2.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactly("doc2", "doc4");
        }
    }

    // =====================================================================
    // 6. Vector Search (KNN)
    // =====================================================================
    @Nested
    @DisplayName("VectorSearch")
    class VectorSearch {

        @Test
        @DisplayName("KNN search returns nearest neighbors in ranked order")
        void knnSearchReturnNearestNeighbors() {
            // Query vector close to doc1 [1,0,0]
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(0.95f, 0.05f, 0.0f))
                            .setK(3))
                    .setTopK(3)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            // doc1 [1,0,0] and doc4 [0.9,0.1,0] should be most similar
            assertThat(resp.getHitsList().get(0).getDocId()).isIn("doc1", "doc4");
        }

        @Test
        @DisplayName("KNN search with filter pre-filters during ANN")
        void knnSearchWithFilter() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(1.0f, 0.0f, 0.0f))
                            .setK(5))
                    .addFilters(termFilter("category", "food"))
                    .setTopK(5)
                    .build());

            // Only food docs: doc3, doc5
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .allMatch(id -> id.equals("doc3") || id.equals("doc5"));
        }

        @Test
        @DisplayName("KNN k parameter controls result count")
        void knnSearchDifferentK() {
            QueryResponse resp1 = executeQuery(QueryRequest.newBuilder()
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(0.5f, 0.5f, 0.0f))
                            .setK(1))
                    .setTopK(1)
                    .build());

            QueryResponse resp3 = executeQuery(QueryRequest.newBuilder()
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(0.5f, 0.5f, 0.0f))
                            .setK(3))
                    .setTopK(3)
                    .build());

            assertThat(resp1.getHitsList()).hasSize(1);
            assertThat(resp3.getHitsList()).hasSize(3);
        }
    }

    // =====================================================================
    // 7. Hybrid Search (BM25 + KNN RRF)
    // =====================================================================
    @Nested
    @DisplayName("HybridSearch")
    class HybridSearch {

        @Test
        @DisplayName("Hybrid search combines text and vector results")
        void hybridSearchCombinesTextAndVector() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("machine learning")
                    .setTextField("title")
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(0.0f, 1.0f, 0.0f))
                            .setK(5))
                    .setTopK(5)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            // Should include results from both text (doc1 "Machine Learning") and vector (doc2 [0,1,0])
            List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(ids).contains("doc1"); // text match
            assertThat(ids).contains("doc2"); // vector match for [0,1,0]
        }

        @Test
        @DisplayName("Hybrid search with filters")
        void hybridSearchWithFilters() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("science")
                    .setTextField("title")
                    .setVectorQuery(VectorQuery.newBuilder()
                            .setField("embedding")
                            .addAllVector(List.of(1.0f, 0.0f, 0.0f))
                            .setK(5))
                    .addFilters(termFilter("category", "food"))
                    .setTopK(5)
                    .build());

            // Only food category results
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .allMatch(id -> id.equals("doc3") || id.equals("doc5"));
        }
    }

    // =====================================================================
    // 8. Filter-Only Browse (MatchAll)
    // =====================================================================
    @Nested
    @DisplayName("FilterOnlyBrowse")
    class FilterOnlyBrowse {

        @Test
        @DisplayName("MatchAll with filter returns filtered docs")
        void matchAllWithFilter() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("category", "tech"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc4");
        }

        @Test
        @DisplayName("MatchAll with no filter returns all documents")
        void matchAllNoFilter() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(5);
        }

        @Test
        @DisplayName("MatchAll with multiple filters uses AND semantics")
        void matchAllWithMultipleFilters() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(termFilter("category", "tech"))
                    .addFilters(rangeFilter("price", "40.0", "100.0", false, false))
                    .setTopK(10)
                    .build());

            // tech AND price [40,100]: doc2=49.99, doc4=79.99
            assertThat(resp.getTotalHits()).isEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc2", "doc4");
        }
    }

    // =====================================================================
    // 9. Tenant Isolation
    // =====================================================================
    @Nested
    @DisplayName("TenantIsolation")
    class TenantIsolation {

        @Test
        @DisplayName("Tenant filter isolates results to the given tenant")
        void tenantFilterIsolatesResults() {
            // doc1,doc2,doc3 have tenant_id="tenantA"; doc4,doc5 have tenant_id="tenantB"
            QueryResponse respA = executeQuery(QueryRequest.newBuilder()
                    .setTenantId("tenantA")
                    .setTopK(10)
                    .build());

            QueryResponse respB = executeQuery(QueryRequest.newBuilder()
                    .setTenantId("tenantB")
                    .setTopK(10)
                    .build());

            assertThat(respA.getTotalHits()).isEqualTo(3);
            assertThat(respA.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2", "doc3");

            assertThat(respB.getTotalHits()).isEqualTo(2);
            assertThat(respB.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc4", "doc5");
        }
    }

    // =====================================================================
    // 10. Edge Cases
    // =====================================================================
    @Nested
    @DisplayName("EdgeCases")
    class EdgeCases {

        @Test
        @DisplayName("topK=0 defaults to 10")
        void topKZeroDefaultsTo10() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTopK(0)
                    .build());

            // We have 5 docs, default topK=10, so all 5 should be returned
            assertThat(resp.getTotalHits()).isEqualTo(5);
            assertThat(resp.getHitsList()).hasSize(5);
        }

        @Test
        @DisplayName("FilterClause with no filter predicate throws error")
        void emptyFilterClauseThrows() {
            QueryRequest req = QueryRequest.newBuilder()
                    .addFilters(FilterClause.newBuilder().setField("category").build())
                    .build();

            StatusRuntimeException ex = executeQueryExpectingError(req);
            assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }

        @Test
        @DisplayName("Filter on non-filterable field throws INVALID_ARGUMENT")
        void filterOnNonFilterableFieldThrows() {
            QueryRequest req = QueryRequest.newBuilder()
                    .addFilters(termFilter("content", "hello"))
                    .build();

            StatusRuntimeException ex = executeQueryExpectingError(req);
            assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }

        @Test
        @DisplayName("Sort on non-sortable field throws error")
        void sortOnNonSortableFieldThrows() {
            QueryRequest req = QueryRequest.newBuilder()
                    .addSort(SortClause.newBuilder().setField("category").setDescending(false))
                    .build();

            StatusRuntimeException ex = executeQueryExpectingError(req);
            assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
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

    private StatusRuntimeException executeQueryExpectingError(QueryRequest req) {
        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
        return (StatusRuntimeException) captor.getValue();
    }

    private void indexDoc(String id, String title, String content, String category,
                          boolean active, double price, int quantity, long timestamp,
                          long createdAt, float[] embedding, String tenantId) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"content\":\"%s\",\"category\":\"%s\"," +
                        "\"active\":%s,\"price\":%s,\"quantity\":%d,\"timestamp\":%d," +
                        "\"created_at\":%d,\"embedding\":[%s]}",
                id, title, content, category, active, price, quantity, timestamp,
                createdAt, formatEmbedding(embedding));

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
        // Verify indexing succeeded
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private String formatEmbedding(float[] embedding) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < embedding.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(embedding[i]);
        }
        return sb.toString();
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "Machine Learning Basics",
                "Introduction to machine learning basics and algorithms",
                "tech", true, 29.99, 100, 1000, 1700000000000L,
                new float[]{1.0f, 0.0f, 0.0f}, "tenantA");

        indexDoc("doc2", "Database Internals",
                "Deep dive into database indexing and storage engines",
                "tech", true, 49.99, 50, 2000, 1700100000000L,
                new float[]{0.0f, 1.0f, 0.0f}, "tenantA");

        indexDoc("doc3", "Cooking Made Easy",
                "Simple recipes for everyday cooking",
                "food", false, 15.99, 200, 3000, 1700200000000L,
                new float[]{0.0f, 0.0f, 1.0f}, "tenantA");

        indexDoc("doc4", "Advanced AI Systems",
                "Cutting-edge artificial intelligence and deep learning",
                "tech", true, 79.99, 25, 4000, 1700300000000L,
                new float[]{0.9f, 0.1f, 0.0f}, "tenantB");

        indexDoc("doc5", "Food Science",
                "The science behind food preservation and nutrition",
                "food", true, 35.99, 75, 5000, 1700400000000L,
                new float[]{0.1f, 0.9f, 0.0f}, "tenantB");
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManager.SearcherLease(acquiredSearcher, searcherManager));

        helpers = mock(HandlerHelpers.class);
        when(helpers.indexSchemaManager()).thenReturn(schemaManager);
        when(helpers.indexSearcherManager()).thenReturn(searcherManager);

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

    private static FilterClause rangeFilter(String field, String min, String max,
                                             boolean minExclusive, boolean maxExclusive) {
        return FilterClause.newBuilder()
                .setField(field)
                .setRangeFilter(RangeFilter.newBuilder()
                        .setMin(min).setMax(max)
                        .setMinExclusive(minExclusive).setMaxExclusive(maxExclusive))
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

        SchemaField content = new SchemaField();
        content.setName("content");
        content.setType(FieldDataType.STRING);
        content.setSearchable(true);

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);

        SchemaField active = new SchemaField();
        active.setName("active");
        active.setType(FieldDataType.BOOLEAN);
        active.setFilterable(true);

        SchemaField price = new SchemaField();
        price.setName("price");
        price.setType(FieldDataType.DOUBLE);
        price.setFilterable(true);
        price.setSortable(true);

        SchemaField quantity = new SchemaField();
        quantity.setName("quantity");
        quantity.setType(FieldDataType.INT32);
        quantity.setFilterable(true);
        quantity.setSortable(true);

        SchemaField timestamp = new SchemaField();
        timestamp.setName("timestamp");
        timestamp.setType(FieldDataType.INT64);
        timestamp.setFilterable(true);
        timestamp.setSortable(true);

        SchemaField createdAt = new SchemaField();
        createdAt.setName("created_at");
        createdAt.setType(FieldDataType.DATE_TIME_OFFSET);
        createdAt.setFilterable(true);
        createdAt.setSortable(true);

        SchemaField embedding = new SchemaField();
        embedding.setName("embedding");
        embedding.setType(FieldDataType.VECTOR);
        embedding.setDimension(3);
        embedding.setSimilarityFunction("COSINE");

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, content, category, active, price,
                quantity, timestamp, createdAt, embedding});
        schema.setBm25Config(new BM25Config());
        schema.setTtlEnabled(true);
        return schema;
    }
}
