package vectordemo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub;
import com.jigyasa.dp.search.protocol.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Jigyasa Example 03 — Vector &amp; Hybrid Search (Java).
 *
 * Demonstrates pure KNN, pure BM25, and hybrid (RRF fusion) search
 * on a collection with 16-dimensional embeddings.
 *
 * Usage: gradle run
 */
public class VectorAndHybrid {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COLLECTION = "vector-hybrid-demo-java";

    // Query vector: slightly perturbed version of article a4
    private static final float[] QUERY_VECTOR = {
        0.44f, -0.66f, 0.90f, 0.22f, -0.35f, 0.55f, -0.77f, 0.13f,
        0.92f, -0.44f, 0.66f, -0.88f, 0.24f, 0.35f, -0.55f, 0.77f,
    };

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        // ── 1. Create collection with vector field ──────────────────
        System.out.println("1. Create collection with VECTOR field + HNSW config");
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode fields = schema.putArray("fields");
        fields.add(field("id",        "STRING", true, false, true, false, 0));
        fields.add(field("title",     "STRING", false, true, false, false, 0));
        fields.add(field("content",   "STRING", false, true, false, false, 0));
        fields.add(field("author",    "STRING", false, false, true, false, 0));
        fields.add(field("category",  "STRING", false, false, true, false, 0));
        fields.add(field("published_at", "STRING", false, false, true, false, 0));
        fields.add(field("embedding", "VECTOR", false, false, false, false, 16));
        ObjectNode hnsw = schema.putObject("hnswConfig");
        hnsw.put("maxConn", 16);
        hnsw.put("beamWidth", 100);

        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setIndexSchema(MAPPER.writeValueAsString(schema))
                    .build());
            System.out.println("   ✓ Created\n");
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.println("   ✓ Already exists\n");
            } else {
                throw e;
            }
        }

        // ── 2. Index documents from JSONL ───────────────────────────
        System.out.println("2. Index articles with embeddings");
        Path dataFile = Paths.get(System.getProperty("user.dir"), "..", "..", "data",
                "articles-with-embeddings.jsonl");
        List<IndexItem> items = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isBlank()) {
                    items.add(IndexItem.newBuilder().setDocument(line).build());
                }
            }
        }
        IndexResponse indexResp = stub.index(IndexRequest.newBuilder()
                .setCollection(COLLECTION)
                .addAllItem(items)
                .build());
        System.out.println("   ✓ Indexed " + indexResp.getItemResponseCount() + " documents\n");

        Thread.sleep(1000);

        // ── 3. Pure KNN vector search ───────────────────────────────
        System.out.println("3. Pure KNN vector search (query ≈ article a4 embedding)");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setVectorQuery(vectorQuery(5))
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        printHits(resp, "Vector-only results");

        // ── 4. Pure text search (BM25) ──────────────────────────────
        System.out.println("4. Pure BM25 text search for 'LLM agents memory planning'");
        resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("LLM agents memory planning")
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        printHits(resp, "Text-only (BM25) results");

        // ── 5. Hybrid search (text_weight=0.5) ─────────────────────
        System.out.println("5. Hybrid search: text_weight=0.5 (balanced RRF fusion)");
        resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTextQuery("LLM agents memory planning")
                .setVectorQuery(vectorQuery(5))
                .setTextWeight(0.5f)
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        printHits(resp, "Hybrid (0.5) results");

        // ── 6. Compare different text_weight values ─────────────────
        System.out.println("6. Hybrid with different text_weight values");
        for (float weight : new float[]{0.3f, 0.7f}) {
            resp = stub.query(QueryRequest.newBuilder()
                    .setCollection(COLLECTION)
                    .setTextQuery("LLM agents memory planning")
                    .setVectorQuery(vectorQuery(5))
                    .setTextWeight(weight)
                    .setIncludeSource(true)
                    .setTopK(5)
                    .build());
            String label = String.format("text_weight=%.1f (%s)",
                    weight, weight < 0.5f ? "more vector" : "more text");
            printHits(resp, label);
        }

        // ── 7. Vector search with filter ────────────────────────────
        System.out.println("7. Vector search + filter (category='ai')");
        resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setVectorQuery(vectorQuery(5))
                .addFilters(FilterClause.newBuilder()
                        .setField("category")
                        .setTermFilter(TermFilter.newBuilder().setValue("ai").build())
                        .build())
                .setIncludeSource(true)
                .setTopK(5)
                .build());
        printHits(resp, "Vector + category='ai' filter");

        System.out.println("✅ Vector & hybrid search demo complete!");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static VectorQuery vectorQuery(int k) {
        VectorQuery.Builder vq = VectorQuery.newBuilder()
                .setField("embedding")
                .setK(k);
        for (float v : QUERY_VECTOR) {
            vq.addVector(v);
        }
        return vq.build();
    }

    private static void printHits(QueryResponse resp, String label) throws Exception {
        System.out.println("   --- " + label + " ---");
        System.out.println("   Total hits: " + resp.getTotalHits());
        int rank = 1;
        for (QueryHit hit : resp.getHitsList()) {
            JsonNode doc = MAPPER.readTree(hit.getSource());
            System.out.printf("   %d. [%.4f] %3s | %s%n",
                    rank++, hit.getScore(), doc.get("id").asText(), doc.get("title").asText());
        }
        System.out.println();
    }

    private static ObjectNode field(String name, String type, boolean key,
                                     boolean searchable, boolean filterable,
                                     boolean sortable, int dimensions) {
        ObjectNode f = MAPPER.createObjectNode();
        f.put("name", name);
        f.put("type", type);
        if (key) f.put("key", true);
        if (searchable) f.put("searchable", true);
        if (filterable) f.put("filterable", true);
        if (sortable) f.put("sortable", true);
        if (dimensions > 0) f.put("dimensions", dimensions);
        return f;
    }
}
