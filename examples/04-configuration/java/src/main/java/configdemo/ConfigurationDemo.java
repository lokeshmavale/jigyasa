package configdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.jigyasa.dp.search.protocol.*;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Jigyasa 04 — Configuration: Memory Tiers, TTL &amp; Recency Decay.
 *
 * <p>Demonstrates memory tiers (SEMANTIC / EPISODIC / WORKING), custom TTL
 * overrides, refresh policies, and recency-decay boosted queries.
 *
 * <p>Run: {@code ./gradlew run}
 */
public class ConfigurationDemo {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ADDR = "localhost";
    private static final int PORT = 50051;
    private static final String COLLECTION = "config-demo-java";

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(ADDR, PORT)
                .usePlaintext()
                .build();

        JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        try {
            run(stub);
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void run(JigyasaDataPlaneServiceBlockingStub stub) throws Exception {

        // ── 1. Health check ─────────────────────────────────────────────────
        header("1. Health Check");
        HealthResponse health = stub.health(HealthRequest.getDefaultInstance());
        System.out.println("  Status: " + health.getStatus());

        // ── 2. Create TTL-enabled collection ────────────────────────────────
        header("2. Create TTL-Enabled Collection");
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("ttlEnabled", true);
        ArrayNode fields = schema.putArray("fields");
        fields.add(field("id",         "STRING",  true,  false, true,  false));
        fields.add(field("title",      "STRING",  false, true,  false, true));
        fields.add(field("body",       "STRING",  false, true,  false, false));
        fields.add(field("category",   "STRING",  false, false, true,  false));
        fields.add(field("tier_label", "STRING",  false, false, true,  false));
        fields.add(field("priority",   "INT32", false, false, true,  true));

        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setIndexSchema(MAPPER.writeValueAsString(schema))
                    .build());
            System.out.println("  ✓ Collection '" + COLLECTION + "' created with ttlEnabled=true");
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.println("  ✓ Collection '" + COLLECTION + "' already exists");
            } else {
                throw e;
            }
        }

        // ── 3. Index documents across memory tiers ──────────────────────────
        header("3. Index Documents Across Memory Tiers");

        // SEMANTIC — permanent knowledge
        List<IndexItem> semanticItems = List.of(
                doc("s1", "Company Mission Statement",
                        "Our mission is to organise the world's information and make it universally accessible.",
                        "corporate", "semantic", 10, MemoryTier.SEMANTIC, 0),
                doc("s2", "Product Architecture Overview",
                        "The platform is built on a microservices architecture with gRPC communication.",
                        "engineering", "semantic", 9, MemoryTier.SEMANTIC, 0)
        );

        // EPISODIC — session context (24h default TTL)
        List<IndexItem> episodicItems = List.of(
                doc("e1", "User asked about pricing",
                        "The user inquired about enterprise pricing tiers during today's demo call.",
                        "conversation", "episodic", 7, MemoryTier.EPISODIC, 0),
                doc("e2", "Meeting notes: Q3 planning",
                        "Team agreed to prioritise search latency improvements in Q3.",
                        "meeting", "episodic", 6, MemoryTier.EPISODIC, 0)
        );

        // WORKING — transient scratchpad (5 min default TTL)
        List<IndexItem> workingItems = List.of(
                doc("w1", "Draft response to pricing question",
                        "Suggest the growth plan at $499/month with custom SLA.",
                        "draft", "working", 5, MemoryTier.WORKING, 0),
                doc("w2", "Temp calculation: projected users",
                        "Based on current growth rate, projected 50k users by end of Q3.",
                        "scratch", "working", 3, MemoryTier.WORKING, 0)
        );

        IndexRequest.Builder indexReq = IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .setRefresh(RefreshPolicy.WAIT_FOR);
        semanticItems.forEach(indexReq::addItem);
        episodicItems.forEach(indexReq::addItem);
        workingItems.forEach(indexReq::addItem);

        IndexResponse indexResp = stub.index(indexReq.build());
        System.out.println("  ✓ Indexed " + indexResp.getItemResponseCount()
                + " documents (WAIT_FOR refresh)");
        System.out.println("    SEMANTIC: " + semanticItems.size() + " docs (permanent)");
        System.out.println("    EPISODIC: " + episodicItems.size() + " docs (24h TTL)");
        System.out.println("    WORKING:  " + workingItems.size() + " docs (5min TTL)");

        // ── 4. Custom TTL override ──────────────────────────────────────────
        header("4. Custom TTL Override");
        IndexItem customTtlItem = doc("e3", "Urgent follow-up reminder",
                "Send pricing proposal to client within 1 hour.",
                "reminder", "episodic", 8, MemoryTier.EPISODIC, 3600);

        stub.index(IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .setRefresh(RefreshPolicy.WAIT_FOR)
                .addItem(customTtlItem)
                .build());
        System.out.println("  ✓ Indexed EPISODIC doc with custom TTL = 3600s (1 hour)");
        System.out.println("    Default EPISODIC TTL is 24h — this doc expires sooner");

        // ── 5. Refresh policies ─────────────────────────────────────────────
        header("5. Refresh Policies");

        // NONE — fire-and-forget
        stub.index(IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .setRefresh(RefreshPolicy.NONE)
                .addItem(doc("b1", "Bulk-loaded record",
                        "This document was indexed with NONE refresh policy.",
                        "bulk", "semantic", 1, MemoryTier.SEMANTIC, 0))
                .build());
        System.out.println("  ✓ NONE:      fire-and-forget (not immediately searchable)");

        // IMMEDIATE — force flush
        stub.index(IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .setRefresh(RefreshPolicy.IMMEDIATE)
                .addItem(doc("i1", "Critical alert: system update",
                        "Production deployment v2.5 completed successfully.",
                        "alert", "semantic", 10, MemoryTier.SEMANTIC, 0))
                .build());
        System.out.println("  ✓ IMMEDIATE: forced flush (searchable right now)");
        System.out.println("  ✓ WAIT_FOR:  used in step 3 (blocks until searchable)");

        Thread.sleep(1000); // allow NONE-refreshed doc to become visible

        // ── 6. Query without recency decay ──────────────────────────────────
        header("6. Query Without Recency Decay");
        QueryResponse qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("pricing")
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        printHits(qr);

        // ── 7. Query with recency decay (1-hour half-life) ─────────────────
        header("7. Query With Recency Decay (half-life = 1 hour)");
        qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("pricing")
                .setIncludeSource(true)
                .setTopK(5)
                .setRecencyDecay(RecencyDecay.newBuilder()
                        .setHalfLifeSeconds(3600)
                        .build())
                .build());
        printHits(qr);
        System.out.println("\n  ↑ Recently indexed documents are boosted toward the top.");

        // ── 8. Aggressive recency decay (5-minute half-life) ────────────────
        header("8. Aggressive Recency Decay (half-life = 5 minutes)");
        qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("pricing")
                .setIncludeSource(true)
                .setTopK(5)
                .setRecencyDecay(RecencyDecay.newBuilder()
                        .setHalfLifeSeconds(300)
                        .build())
                .build());
        printHits(qr);
        System.out.println("\n  ↑ With a 5-minute half-life, only very recent docs keep high boost.");

        // ── 9. Full collection query with gentle decay ──────────────────────
        header("9. Full Collection Query (all tiers)");
        qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("system architecture platform")
                .setIncludeSource(true)
                .setTopK(10)
                .setRecencyDecay(RecencyDecay.newBuilder()
                        .setHalfLifeSeconds(86400)
                        .build())
                .build());
        printHits(qr);

        // ── 10. Document count ──────────────────────────────────────────────
        header("10. Document Count");
        CountResponse count = stub.count(CountRequest.newBuilder()
                .setCollection(COLLECTION)
                .build());
        System.out.println("  Total documents in '" + COLLECTION + "': " + count.getCount());
        System.out.println("  (WORKING-tier docs will disappear after ~5 minutes)");

        System.out.println("\n✓ Configuration demo complete.");
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static void header(String title) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }

    private static void printHits(QueryResponse resp) throws Exception {
        System.out.println("  Total hits: " + resp.getTotalHits());
        for (QueryHit hit : resp.getHitsList()) {
            var doc = MAPPER.readTree(hit.getSource());
            String tierLabel = doc.has("tier_label") ? doc.get("tier_label").asText() : "";
            System.out.printf("  [%.4f] %s", hit.getScore(), doc.get("title").asText());
            if (!tierLabel.isEmpty()) {
                System.out.printf("  (tier: %s)", tierLabel);
            }
            System.out.println();
        }
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

    private static IndexItem doc(String id, String title, String body,
                                  String category, String tierLabel, int priority,
                                  MemoryTier tier, int ttlSeconds) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("id", id);
            node.put("title", title);
            node.put("body", body);
            node.put("category", category);
            node.put("tier_label", tierLabel);
            node.put("priority", priority);
            IndexItem.Builder builder = IndexItem.newBuilder()
                    .setDocument(MAPPER.writeValueAsString(node))
                    .setMemoryTier(tier);
            if (ttlSeconds > 0) {
                builder.setTtlSeconds(ttlSeconds);
            }
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
