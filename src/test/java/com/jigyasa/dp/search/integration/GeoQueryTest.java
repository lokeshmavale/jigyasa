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
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.GeoBoundingBoxFilter;
import com.jigyasa.dp.search.protocol.GeoDistanceFilter;
import com.jigyasa.dp.search.protocol.GeoSortOrigin;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.SortClause;
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
 * Integration tests for geo-point query capabilities (distance filter, bounding box, geo sort).
 */
class GeoQueryTest {

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
    // Geo Distance Filter
    // =====================================================================
    @Nested
    @DisplayName("GeoDistanceFilter")
    class GeoDistanceFilterTests {

        @Test
        @DisplayName("Within 100km of NYC returns only NYC")
        void geoDistanceFilter_withinRadius() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("location", 40.7128, -74.0060, 100_000))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).containsExactly("doc1");
        }

        @Test
        @DisplayName("Within 10000km of London returns London, NYC, Mumbai")
        void geoDistanceFilter_largeRadius() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("location", 51.5074, -0.1278, 10_000_000))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(3);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc1", "doc3", "doc5");
        }

        @Test
        @DisplayName("Very large radius matches all 5 documents")
        void geoDistanceFilter_allMatch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("location", 0, 0, 20_000_000))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(5);
        }

        @Test
        @DisplayName("Geo distance filter combined with text query")
        void geoDistanceFilter_plusTextQuery() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("city")
                    .setTextField("description")
                    .addFilters(geoDistanceFilter("location", 40.7128, -74.0060, 10_000_000))
                    .setTopK(10)
                    .build());

            // Text query "city" matches docs containing "city"; geo filter constrains radius
            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("Geo distance filter on non-geo field throws error")
        void geoDistanceFilter_invalidType_throws() {
            StatusRuntimeException err = executeQueryExpectingError(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("description", 40.7128, -74.0060, 100_000))
                    .setTopK(10)
                    .build());

            assertThat(err.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
    }

    // =====================================================================
    // Geo Bounding Box Filter
    // =====================================================================
    @Nested
    @DisplayName("GeoBoundingBoxFilter")
    class GeoBoundingBoxFilterTests {

        @Test
        @DisplayName("Bounding box covering North America returns NYC and LA")
        void geoBoundingBoxFilter_northAmerica() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoBoundingBoxFilter("location", 50, -125, 25, -65))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2");
        }

        @Test
        @DisplayName("Bounding box in ocean returns no results")
        void geoBoundingBoxFilter_noMatch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoBoundingBoxFilter("location", 10, -170, 5, -160))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(0);
        }
    }

    // =====================================================================
    // Geo Distance Sort
    // =====================================================================
    @Nested
    @DisplayName("GeoDistanceSort")
    class GeoDistanceSortTests {

        @Test
        @DisplayName("Sort by distance from London — London is first")
        void geoDistanceSort_nearestToLondon() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("location", 0, 0, 20_000_000)) // match all
                    .addSort(SortClause.newBuilder()
                            .setField("location")
                            .setGeoOrigin(GeoSortOrigin.newBuilder().setLat(51.5074).setLon(-0.1278))
                            .setDescending(false))
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(5);
            List<String> docIds = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
            assertThat(docIds.get(0)).isEqualTo("doc3"); // London is nearest to London
        }

        @Test
        @DisplayName("Descending geo sort throws error")
        void geoDistanceSort_descendingThrows() {
            StatusRuntimeException err = executeQueryExpectingError(QueryRequest.newBuilder()
                    .addFilters(geoDistanceFilter("location", 0, 0, 20_000_000))
                    .addSort(SortClause.newBuilder()
                            .setField("location")
                            .setGeoOrigin(GeoSortOrigin.newBuilder().setLat(51.5074).setLon(-0.1278))
                            .setDescending(true))
                    .setTopK(10)
                    .build());

            assertThat(err.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
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

    private void indexDoc(String id, String description, double lat, double lon) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"description\":\"%s\",\"location\":{\"lat\":%s,\"lon\":%s}}",
                id, description, lat, lon);

        IndexItem.Builder itemBuilder = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json);
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(itemBuilder.build())
                .build();
        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "New York city skyline", 40.7128, -74.0060);
        indexDoc("doc2", "Los Angeles sunny beaches", 34.0522, -118.2437);
        indexDoc("doc3", "London city bridges", 51.5074, -0.1278);
        indexDoc("doc4", "Tokyo neon lights", 35.6762, 139.6503);
        indexDoc("doc5", "Mumbai bustling city streets", 19.0760, 72.8777);
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManagerISCH.SearcherLease(acquiredSearcher, searcherManager));

        helpers = mock(HandlerHelpers.class);
        when(helpers.getIndexSchemaManager()).thenReturn(schemaManager);
        when(helpers.getIndexSearcherManager()).thenReturn(searcherManager);

        CollectionRegistry registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        handler = new QueryRequestHandler(registry);
    }

    private static FilterClause geoDistanceFilter(String field, double lat, double lon, double distanceMeters) {
        return FilterClause.newBuilder()
                .setField(field)
                .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                        .setLat(lat).setLon(lon).setDistanceMeters(distanceMeters))
                .build();
    }

    private static FilterClause geoBoundingBoxFilter(String field, double topLat, double leftLon,
                                                      double bottomLat, double rightLon) {
        return FilterClause.newBuilder()
                .setField(field)
                .setGeoBoundingBoxFilter(GeoBoundingBoxFilter.newBuilder()
                        .setTopLat(topLat).setLeftLon(leftLon)
                        .setBottomLat(bottomLat).setRightLon(rightLon))
                .build();
    }

    private IndexSchema buildSchema() {
        SchemaField id = new SchemaField();
        id.setName("id");
        id.setType(FieldDataType.STRING);
        id.setKey(true);
        id.setFilterable(true);

        SchemaField description = new SchemaField();
        description.setName("description");
        description.setType(FieldDataType.STRING);
        description.setSearchable(true);
        description.setFilterable(true);

        SchemaField location = new SchemaField();
        location.setName("location");
        location.setType(FieldDataType.GEO_POINT);
        location.setFilterable(true);
        location.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, description, location});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
