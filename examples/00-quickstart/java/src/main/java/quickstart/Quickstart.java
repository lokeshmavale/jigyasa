package quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub;
import com.jigyasa.dp.search.protocol.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Jigyasa Quickstart — 5 minutes from zero to search.
 *
 * Usage: gradle run
 */
public class Quickstart {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        // 1. Health check
        System.out.println("1. Health check");
        HealthResponse health = stub.health(HealthRequest.getDefaultInstance());
        System.out.println("   Status: " + health.getStatus());

        // 2. Create collection
        System.out.println("\n2. Create collection 'quickstart-java'");
        String schema = MAPPER.writeValueAsString(MAPPER.createObjectNode().set("fields",
                MAPPER.createArrayNode()
                        .add(field("id", "STRING", true, false, true, false))
                        .add(field("title", "STRING", false, true, false, true))
                        .add(field("body", "STRING", false, true, false, false))
                        .add(field("category", "STRING", false, false, true, false))
        ));
        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection("quickstart-java")
                    .setIndexSchema(schema)
                    .build());
            System.out.println("   ✓ Created");
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.println("   ✓ Already exists");
            } else {
                throw e;
            }
        }

        // 3. Index documents
        System.out.println("\n3. Index 5 documents");
        List<IndexItem> items = List.of(
                doc("1", "Getting Started with Lucene", "Apache Lucene is a high-performance search engine library written in Java", "search"),
                doc("2", "Vector Search Fundamentals", "HNSW algorithm enables fast approximate nearest neighbor search", "search"),
                doc("3", "Building AI Agents", "Autonomous agents use LLMs to reason, plan, and take actions", "ai"),
                doc("4", "gRPC Protocol Guide", "gRPC uses HTTP/2 and Protocol Buffers for efficient communication", "networking"),
                doc("5", "Memory for LLM Agents", "Agents need persistent memory to recall past conversations and facts", "ai")
        );
        IndexResponse indexResp = stub.index(IndexRequest.newBuilder()
                .setCollection("quickstart-java")
                .addAllItem(items)
                .build());
        System.out.println("   ✓ Indexed " + indexResp.getItemResponseCount() + " documents");

        Thread.sleep(1000); // Wait for NRT refresh

        // 4. Search
        System.out.println("\n4. Search for 'search engine library'");
        QueryResponse queryResp = stub.query(QueryRequest.newBuilder()
                .setCollection("quickstart-java")
                .setTextQuery("search engine library")
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        System.out.println("   Total hits: " + queryResp.getTotalHits());
        for (QueryHit hit : queryResp.getHitsList()) {
            var doc = MAPPER.readTree(hit.getSource());
            System.out.printf("   [%.3f] %s%n", hit.getScore(), doc.get("title").asText());
        }

        // 5. Count
        System.out.println("\n5. Total documents in collection");
        CountResponse count = stub.count(CountRequest.newBuilder()
                .setCollection("quickstart-java").build());
        System.out.println("   Count: " + count.getCount());

        System.out.println("\n✅ Quickstart complete!");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static ObjectNode field(String name, String type, boolean key,
                                     boolean searchable, boolean filterable, boolean sortable) {
        ObjectNode f = MAPPER.createObjectNode();
        f.put("name", name);
        f.put("type", type);
        if (key) f.put("key", true);
        if (searchable) f.put("searchable", true);
        if (filterable) f.put("filterable", true);
        if (sortable) f.put("sortable", true);
        return f;
    }

    private static IndexItem doc(String id, String title, String body, String category) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("id", id);
            node.put("title", title);
            node.put("body", body);
            node.put("category", category);
            return IndexItem.newBuilder()
                    .setDocument(MAPPER.writeValueAsString(node))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
