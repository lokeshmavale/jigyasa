package ecommerce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.jigyasa.dp.search.protocol.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Non-interactive e-commerce search demo that runs all scenarios sequentially.
 */
public class EcommerceApp {

    private static final String COLLECTION = "shop";
    private static final String SERVER = "localhost:50051";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(SERVER)
                .usePlaintext()
                .build();
        JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        try {
            System.out.println("=== E-Commerce Search Demo (Java) ===\n");

            setupCollection(stub);
            indexProducts(stub);

            scenario1_textSearch(stub);
            scenario2_browseCategory(stub);
            scenario3_priceRange(stub);
            scenario4_nearbyStores(stub);
            scenario5_advancedSearch(stub);
            scenario6_productCount(stub);

            System.out.println("=== Demo Complete ===");
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // -----------------------------------------------------------------------
    // Setup
    // -----------------------------------------------------------------------

    private static String buildSchema() throws Exception {
        Map<String, Object> schema = new LinkedHashMap<>();
        List<Map<String, Object>> fields = new ArrayList<>();

        fields.add(field("id", "STRING", Map.of("key", true, "filterable", true)));
        fields.add(field("title", "STRING", Map.of("searchable", true, "sortable", true)));
        fields.add(field("description", "STRING", Map.of("searchable", true)));
        fields.add(field("category", "STRING", Map.of("filterable", true, "sortable", true)));
        fields.add(field("brand", "STRING", Map.of("filterable", true)));
        fields.add(field("price", "DOUBLE", Map.of("filterable", true, "sortable", true)));
        fields.add(field("rating", "DOUBLE", Map.of("filterable", true, "sortable", true)));
        fields.add(field("in_stock", "INT32", Map.of("filterable", true)));
        fields.add(field("location", "GEO_POINT", Map.of("filterable", true)));

        schema.put("fields", fields);
        return mapper.writeValueAsString(schema);
    }

    private static Map<String, Object> field(String name, String type, Map<String, Object> opts) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("name", name);
        f.put("type", type);
        f.putAll(opts);
        return f;
    }

    private static void setupCollection(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) throws Exception {
        System.out.println("--- Creating collection ---");
        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setIndexSchema(buildSchema())
                    .build());
            System.out.println("Collection '" + COLLECTION + "' created.");
        } catch (StatusRuntimeException e) {
            if (e.getMessage().toLowerCase().contains("already exists")) {
                System.out.println("Collection '" + COLLECTION + "' already exists, reusing.");
            } else {
                throw e;
            }
        }
    }

    private static void indexProducts(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) throws Exception {
        System.out.println("\n--- Indexing products ---");
        Path dataFile = Paths.get(System.getProperty("user.dir"), "..", "..", "data", "products.jsonl");
        List<String> lines = Files.readAllLines(dataFile);

        IndexRequest.Builder req = IndexRequest.newBuilder().setCollection(COLLECTION);
        int count = 0;
        for (String line : lines) {
            line = line.trim();
            if (!line.isEmpty()) {
                JsonNode node = mapper.readTree(line);
                if (node.has("in_stock")) {
                    ((com.fasterxml.jackson.databind.node.ObjectNode) node).put("in_stock", node.get("in_stock").asBoolean() ? 1 : 0);
                }
                req.addItem(IndexItem.newBuilder().setDocument(mapper.writeValueAsString(node)).build());
                count++;
            }
        }
        stub.index(req.build());
        System.out.println("Indexed " + count + " products.\n");
    }

    // -----------------------------------------------------------------------
    // Scenarios
    // -----------------------------------------------------------------------

    private static void scenario1_textSearch(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 1: Text Search for \"laptop\" ---");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setTextQuery("laptop")
                .setCollection(COLLECTION)
                .setTopK(5)
                .setIncludeSource(true)
                .build());
        printResults(resp);
    }

    private static void scenario2_browseCategory(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 2: Browse Category \"Electronics\" ---");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("category")
                        .setTermFilter(TermFilter.newBuilder().setValue("electronics").build())
                        .build())
                .addSort(SortClause.newBuilder().setField("rating").setDescending(true).build())
                .setTopK(10)
                .setIncludeSource(true)
                .build());
        printResults(resp);
    }

    private static void scenario3_priceRange(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 3: Price Range $25 - $75 ---");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("price")
                        .setRangeFilter(RangeFilter.newBuilder()
                                .setMin("25")
                                .setMax("75")
                                .build())
                        .build())
                .addSort(SortClause.newBuilder().setField("price").setDescending(false).build())
                .setTopK(20)
                .setIncludeSource(true)
                .build());
        printResults(resp);
    }

    private static void scenario4_nearbyStores(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 4: Nearby Stores (10km from San Francisco) ---");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("location")
                        .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                                .setLat(37.7749)
                                .setLon(-122.4194)
                                .setDistanceMeters(10000)
                                .build())
                        .build())
                .setTopK(20)
                .setIncludeSource(true)
                .build());
        printResults(resp);
    }

    private static void scenario5_advancedSearch(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 5: Advanced Search (Electronics, $50-$200, sorted by price) ---");

        FilterClause categoryFilter = FilterClause.newBuilder()
                .setField("category")
                .setTermFilter(TermFilter.newBuilder().setValue("electronics").build())
                .build();
        FilterClause priceFilter = FilterClause.newBuilder()
                .setField("price")
                .setRangeFilter(RangeFilter.newBuilder().setMin("50").setMax("200").build())
                .build();

        FilterClause compound = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .setOperator(CompoundFilter.Operator.AND)
                        .addMust(categoryFilter)
                        .addMust(priceFilter)
                        .build())
                .build();

        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(compound)
                .addSort(SortClause.newBuilder().setField("price").setDescending(false).build())
                .setTopK(20)
                .setIncludeSource(true)
                .build());
        printResults(resp);
    }

    private static void scenario6_productCount(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        System.out.println("--- Scenario 6: Product Count ---");

        CountResponse total = stub.count(CountRequest.newBuilder()
                .setCollection(COLLECTION)
                .build());
        System.out.println("  Total products: " + total.getCount());

        CountResponse electronics = stub.count(CountRequest.newBuilder()
                .setCollection(COLLECTION)
                .addFilters(FilterClause.newBuilder()
                        .setField("category")
                        .setTermFilter(TermFilter.newBuilder().setValue("electronics").build())
                        .build())
                .build());
        System.out.println("  Electronics: " + electronics.getCount());
        System.out.println();
    }

    // -----------------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------------

    private static void printResults(QueryResponse resp) {
        if (resp.getHitsCount() == 0) {
            System.out.println("  No results found.\n");
            return;
        }
        System.out.println("  Found " + resp.getHitsCount() + " result(s):\n");
        int idx = 1;
        for (QueryHit hit : resp.getHitsList()) {
            try {
                JsonNode src = mapper.readTree(hit.getSource());
                String title = src.path("title").asText("N/A");
                double price = src.path("price").asDouble(0);
                double rating = src.path("rating").asDouble(0);
                String brand = src.path("brand").asText("");
                String category = src.path("category").asText("");
                String desc = src.path("description").asText("");

                System.out.printf("  #%-3d %s  ($%.2f)  ★ %.1f%n", idx, title, price, rating);
                if (!brand.isEmpty()) {
                    System.out.printf("       Brand: %s  |  Category: %s%n", brand, category);
                }
                if (!desc.isEmpty()) {
                    System.out.println("       " + (desc.length() > 100 ? desc.substring(0, 100) + "..." : desc));
                }
                System.out.println();
            } catch (Exception e) {
                System.out.println("  #" + idx + " [error parsing source]");
            }
            idx++;
        }
    }
}
