package querycookbook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.jigyasa.dp.search.protocol.*;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Query Cookbook — demonstrates every Jigyasa query type.
 */
public class QueryCookbook {

    private static final String ADDR = "localhost";
    private static final int PORT = 50051;
    private static final String COLLECTION = "cookbook-java";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static void banner(String title) {
        System.out.println("\n" + "=".repeat(60));
        System.out.printf("  %s%n", title);
        System.out.println("=".repeat(60));
    }

    private static void printHits(QueryResponse resp) {
        System.out.printf("  Total hits: %d (exact=%b)%n",
                resp.getTotalHits(), resp.getTotalHitsExact());
        int i = 1;
        for (QueryHit hit : resp.getHitsList()) {
            if (!hit.getSource().isEmpty()) {
                try {
                    JsonNode doc = MAPPER.readTree(hit.getSource());
                    System.out.printf("  %d. [%.4f] %s  ($%s  ★%s)%n", i++,
                            hit.getScore(),
                            doc.path("title").asText(hit.getDocId()),
                            doc.path("price").asText("?"),
                            doc.path("rating").asText("?"));
                } catch (Exception e) {
                    System.out.printf("  %d. [%.4f] doc_id=%s%n", i++,
                            hit.getScore(), hit.getDocId());
                }
            } else {
                System.out.printf("  %d. [%.4f] doc_id=%s%n", i++,
                        hit.getScore(), hit.getDocId());
            }
        }
    }

    private static ObjectNode field(String name, String type) {
        ObjectNode f = MAPPER.createObjectNode();
        f.put("name", name).put("type", type);
        return f;
    }

    // ── Setup ───────────────────────────────────────────────────────────────

    private static void setup(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub)
            throws IOException, InterruptedException {
        banner("Setup — create collection & index products");

        ArrayNode fields = MAPPER.createArrayNode();
        fields.add(field("id", "STRING").put("key", true).put("filterable", true));
        fields.add(field("title", "STRING").put("searchable", true).put("sortable", true));
        fields.add(field("description", "STRING").put("searchable", true));
        fields.add(field("category", "STRING").put("filterable", true).put("sortable", true));
        fields.add(field("brand", "STRING").put("filterable", true));
        fields.add(field("price", "DOUBLE").put("filterable", true).put("sortable", true));
        fields.add(field("rating", "DOUBLE").put("filterable", true).put("sortable", true));
        fields.add(field("in_stock", "INT32").put("filterable", true));
        fields.add(field("location", "GEO_POINT").put("filterable", true));

        ObjectNode schema = MAPPER.createObjectNode();
        schema.set("fields", fields);

        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setIndexSchema(MAPPER.writeValueAsString(schema))
                    .build());
            System.out.printf("  Created collection '%s'%n", COLLECTION);
        } catch (StatusRuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                System.out.printf("  Collection '%s' already exists — reusing%n", COLLECTION);
            } else {
                throw e;
            }
        }

        // Load products from JSONL
        Path dataFile = Path.of(System.getProperty("user.dir"), "..", "..", "data", "products.jsonl");
        List<IndexItem> items = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(dataFile)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isBlank()) {
                    JsonNode node = MAPPER.readTree(line);
                    if (node.has("in_stock")) {
                        ((ObjectNode) node).put("in_stock", node.get("in_stock").asBoolean() ? 1 : 0);
                    }
                    items.add(IndexItem.newBuilder().setDocument(MAPPER.writeValueAsString(node)).build());
                }
            }
        }

        IndexResponse indexResp = stub.index(IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .addAllItem(items)
                .build());
        System.out.printf("  Indexed %d products  (%d responses)%n",
                items.size(), indexResp.getItemResponseCount());
        Thread.sleep(1000); // wait for NRT refresh
    }

    // ── Queries ─────────────────────────────────────────────────────────────

    private static void queryBm25(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("1. BM25 Text Search — \"wireless headphones\"");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("wireless headphones")
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryPhrase(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("2. Phrase Query — \"cast iron skillet\" (slop=0)");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setPhraseQuery("cast iron skillet")
                .setPhraseField("title")
                .setPhraseSlop(0)
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryFuzzy(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("3. Fuzzy Query — \"headphoness\" (typo, max_edits=2)");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setFuzzyQuery("headphoness")
                .setFuzzyField("title")
                .setMaxEdits(2)
                .setPrefixLength(0)
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryPrefix(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("4. Prefix Query — \"wire\" (autocomplete)");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setPrefixQuery("wire")
                .setPrefixField("title")
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryString(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("5. Query String — \"description:organic AND description:natural\"");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setQueryString("description:organic AND description:natural")
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryMatchAll(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("6. Match-All (no query, top_k=20)");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTopK(20)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryTermFilter(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("7. Term Filter — category=\"electronics\"");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("category")
                        .setTermFilter(TermFilter.newBuilder().setValue("electronics")))
                .setTopK(10)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryRangeFilter(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("8. Range Filter — price between $20 and $100");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("price")
                        .setRangeFilter(RangeFilter.newBuilder()
                                .setMin("20").setMax("100")))
                .setTopK(10)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryGeoDistance(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("9. Geo Distance Filter — within 1000km of NYC (40.7128, -74.0060)");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("location")
                        .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                                .setLat(40.7128).setLon(-74.0060)
                                .setDistanceMeters(1_000_000)))
                .setTopK(10)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryCompoundFilter(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("10. Boolean Compound — (electronics OR sports) AND NOT brand=FlexZone");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setCompoundFilter(CompoundFilter.newBuilder()
                                .setOperator(CompoundFilter.Operator.AND)
                                .addShould(FilterClause.newBuilder()
                                        .setField("category")
                                        .setTermFilter(TermFilter.newBuilder().setValue("electronics")))
                                .addShould(FilterClause.newBuilder()
                                        .setField("category")
                                        .setTermFilter(TermFilter.newBuilder().setValue("sports")))
                                .addMustNot(FilterClause.newBuilder()
                                        .setField("brand")
                                        .setTermFilter(TermFilter.newBuilder().setValue("FlexZone")))))
                .setTopK(10)
                .setIncludeSource(true)
                .build()));
    }

    private static void querySort(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("11. Sort — by price descending");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addSort(SortClause.newBuilder()
                        .setField("price").setDescending(true))
                .setTopK(10)
                .setIncludeSource(true)
                .build()));
    }

    private static void queryCombined(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("12. Combined — text + filter + sort");
        printHits(stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("premium quality")
                .addFilters(FilterClause.newBuilder()
                        .setField("price")
                        .setRangeFilter(RangeFilter.newBuilder()
                                .setMin("50").setMax("300")))
                .addSort(SortClause.newBuilder()
                        .setField("rating").setDescending(true))
                .setTopK(5)
                .setIncludeSource(true)
                .build()));
    }

    // ── Main ────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ADDR, PORT)
                .usePlaintext()
                .build();

        try {
            var stub = JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

            setup(stub);

            queryBm25(stub);
            queryPhrase(stub);
            queryFuzzy(stub);
            queryPrefix(stub);
            queryString(stub);
            queryMatchAll(stub);
            queryTermFilter(stub);
            queryRangeFilter(stub);
            queryGeoDistance(stub);
            queryCompoundFilter(stub);
            querySort(stub);
            queryCombined(stub);

            System.out.println("\n" + "=".repeat(60));
            System.out.println("  All 12 queries completed!");
            System.out.println("=".repeat(60) + "\n");
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
