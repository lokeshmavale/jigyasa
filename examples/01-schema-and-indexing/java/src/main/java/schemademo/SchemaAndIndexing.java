package schemademo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import com.jigyasa.dp.search.protocol.*;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Example 01 – Schema Definition & Bulk Indexing (Java)
 *
 * Demonstrates:
 *   1. Creating a collection with a rich schema
 *   2. Bulk-indexing 20 products from a JSONL file
 *   3. Looking up documents by key
 *   4. Counting documents (total and filtered)
 *   5. Checking collection health
 */
public class SchemaAndIndexing {

    private static final String COLLECTION = "products_01";
    private static final String SERVER = "localhost:50051";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Path to shared dataset (relative to the java/ directory)
    private static final Path DATA_FILE =
            Paths.get(System.getProperty("user.dir"), "..", "..", "data", "products.jsonl")
                 .normalize();

    /**
     * Build the index schema as a JSON string.
     */
    private static String buildSchema() throws Exception {
        String schema = """
                {
                  "fields": [
                    {"name": "id",          "type": "STRING",    "key": true, "filterable": true},
                    {"name": "title",       "type": "STRING",    "searchable": true},
                    {"name": "description", "type": "STRING",    "searchable": true},
                    {"name": "category",    "type": "STRING",    "filterable": true},
                    {"name": "brand",       "type": "STRING",    "filterable": true},
                    {"name": "price",       "type": "DOUBLE",    "filterable": true, "sortable": true},
                    {"name": "rating",      "type": "DOUBLE",    "filterable": true, "sortable": true},
                    {"name": "in_stock",    "type": "INT32",     "filterable": true},
                    {"name": "location",    "type": "GEO_POINT", "filterable": true}
                  ]
                }
                """;
        return schema;
    }

    /**
     * Load products from the JSONL file.
     */
    private static List<String> loadProducts() throws IOException {
        List<String> products = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(DATA_FILE)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    // Convert boolean in_stock to int (1/0) for the INT32 field
                    ObjectNode node = (ObjectNode) MAPPER.readTree(line);
                    boolean inStock = node.path("in_stock").asBoolean(false);
                    node.put("in_stock", inStock ? 1 : 0);
                    products.add(MAPPER.writeValueAsString(node));
                }
            }
        }
        return products;
    }

    private static void printSeparator(String title) {
        System.out.println("=".repeat(60));
        System.out.println(title);
        System.out.println("=".repeat(60));
    }

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(SERVER)
                .usePlaintext()
                .build();
        JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        try {
            // ----------------------------------------------------------
            // Step 1 – Create collection with schema
            // ----------------------------------------------------------
            printSeparator("Step 1: Create collection with schema");

            CreateCollectionRequest createReq = CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setIndexSchema(buildSchema())
                    .build();
            try {
                stub.createCollection(createReq);
                System.out.printf("  Collection '%s' created successfully.%n", COLLECTION);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                    System.out.printf("  Collection '%s' already exists – continuing.%n", COLLECTION);
                } else {
                    throw e;
                }
            }
            System.out.println();

            // ----------------------------------------------------------
            // Step 2 – Bulk-index all products
            // ----------------------------------------------------------
            printSeparator("Step 2: Bulk-index products from JSONL");

            List<String> products = loadProducts();
            System.out.printf("  Loaded %d products from %s%n", products.size(), DATA_FILE.getFileName());

            IndexRequest.Builder indexBuilder = IndexRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setRefresh(RefreshPolicy.IMMEDIATE);
            for (String doc : products) {
                indexBuilder.addItem(IndexItem.newBuilder()
                        .setDocument(doc)
                        .setAction(IndexAction.UPDATE)
                        .build());
            }
            stub.index(indexBuilder.build());
            System.out.printf("  Indexed %d documents (refresh=IMMEDIATE).%n", products.size());
            System.out.println();

            // Brief pause to let the index settle
            TimeUnit.SECONDS.sleep(1);

            // ----------------------------------------------------------
            // Step 3 – Lookup documents by key
            // ----------------------------------------------------------
            printSeparator("Step 3: Lookup documents by key");

            LookupRequest lookupReq = LookupRequest.newBuilder()
                    .addDocKeys("p1")
                    .addDocKeys("p10")
                    .addDocKeys("p20")
                    .setCollection(COLLECTION)
                    .build();
            LookupResponse lookupResp = stub.lookup(lookupReq);
            for (String docStr : lookupResp.getDocumentsList()) {
                JsonNode doc = MAPPER.readTree(docStr);
                System.out.printf("  [%s] %s  –  $%.2f%n",
                        doc.get("id").asText(),
                        doc.get("title").asText(),
                        doc.get("price").asDouble());
            }
            System.out.println();

            // ----------------------------------------------------------
            // Step 4 – Count documents (total and filtered)
            // ----------------------------------------------------------
            printSeparator("Step 4: Count documents");

            // Total count
            CountRequest countReq = CountRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .build();
            CountResponse countResp = stub.count(countReq);
            System.out.printf("  Total documents: %d%n", countResp.getCount());

            // Filtered count – only electronics
            CountRequest filterReq = CountRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .addFilters(FilterClause.newBuilder()
                            .setField("category")
                            .setTermFilter(TermFilter.newBuilder().setValue("electronics").build())
                            .build())
                    .build();
            CountResponse filterResp = stub.count(filterReq);
            System.out.printf("  Electronics:     %d%n", filterResp.getCount());
            System.out.println();

            // ----------------------------------------------------------
            // Step 5 – Collection health check
            // ----------------------------------------------------------
            printSeparator("Step 5: Collection health");

            HealthRequest healthReq = HealthRequest.newBuilder().build();
            HealthResponse healthResp = stub.health(healthReq);
            boolean found = false;
            for (CollectionHealth coll : healthResp.getCollectionsList()) {
                if (COLLECTION.equals(coll.getName())) {
                    System.out.printf("  Collection:         %s%n", coll.getName());
                    System.out.printf("  Writer open:        %s%n", coll.getWriterOpen());
                    System.out.printf("  Searcher available: %s%n", coll.getSearcherAvailable());
                    System.out.printf("  Doc count:          %d%n", coll.getDocCount());
                    System.out.printf("  Segment count:      %d%n", coll.getSegmentCount());
                    found = true;
                    break;
                }
            }
            if (!found) {
                System.out.printf("  Collection '%s' not found in health response.%n", COLLECTION);
            }
            System.out.println();

            System.out.println("Done ✓");

        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
