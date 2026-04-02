package opsdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.jigyasa.dp.search.protocol.*;
import com.jigyasa.dp.search.protocol.JigyasaDataPlaneServiceGrpc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Jigyasa Example 05 — Ops &amp; Multi-Tenancy (Java).
 */
public class OpsAndMultitenancy {

    private static final String COLLECTION = "ops_demo_java";
    private static final String ADDR = "localhost:50051";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SCHEMA = "{\"fields\":["
            + "{\"name\":\"id\",\"type\":\"STRING\",\"key\":true,\"filterable\":true},"
            + "{\"name\":\"title\",\"type\":\"STRING\",\"searchable\":true},"
            + "{\"name\":\"category\",\"type\":\"STRING\",\"filterable\":true}"
            + "]}";

    private static final List<Map<String, String>> ACME_DOCS = List.of(
            Map.of("id", "a1", "title", "Acme Cloud Platform",    "category", "software"),
            Map.of("id", "a2", "title", "Acme Rocket Engine",      "category", "hardware"),
            Map.of("id", "a3", "title", "Acme Deployment Toolkit", "category", "software")
    );

    private static final List<Map<String, String>> GLOBEX_DOCS = List.of(
            Map.of("id", "g1", "title", "Globex Strategy Report",  "category", "consulting"),
            Map.of("id", "g2", "title", "Globex Analytics Suite",  "category", "software")
    );

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(ADDR).usePlaintext().build();
        JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        try {
            collectionLifecycle(stub);
            healthMonitoring(stub);
            multiTenancy(stub);
            deleteByQuery(stub);
            forceMerge(stub);
            cleanup(stub);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        System.out.println("\n✅ Done.");
    }

    // ── 1. Collection Lifecycle ─────────────────────────────────────────────

    private static void collectionLifecycle(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("1. Collection Lifecycle");

        System.out.println("Creating collection …");
        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION).setIndexSchema(SCHEMA).build());
            System.out.printf("  ✓ Created '%s'%n", COLLECTION);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.printf("  ✓ '%s' already exists%n", COLLECTION);
            } else {
                throw e;
            }
        }

        ListCollectionsResponse listResp = stub.listCollections(
                ListCollectionsRequest.getDefaultInstance());
        System.out.printf("  Collections: %s%n", listResp.getCollectionsList());

        System.out.println("Closing collection …");
        stub.closeCollection(CloseCollectionRequest.newBuilder()
                .setCollection(COLLECTION).build());
        System.out.printf("  ✓ Closed '%s'%n", COLLECTION);

        System.out.println("Reopening collection (schema from index) …");
        stub.openCollection(OpenCollectionRequest.newBuilder()
                .setCollection(COLLECTION).build());
        System.out.printf("  ✓ Reopened '%s'%n", COLLECTION);

        listResp = stub.listCollections(ListCollectionsRequest.getDefaultInstance());
        System.out.printf("  Collections after reopen: %s%n", listResp.getCollectionsList());
    }

    // ── 2. Health Monitoring ────────────────────────────────────────────────

    private static void healthMonitoring(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("2. Health Monitoring");

        HealthResponse health = stub.health(HealthRequest.getDefaultInstance());
        System.out.printf("  Server status : %s%n", health.getStatus());
        for (CollectionHealth c : health.getCollectionsList()) {
            System.out.printf("  Collection '%s': writer_open=%b, searcher=%b, "
                            + "docs=%d, segments=%d%n",
                    c.getName(), c.getWriterOpen(), c.getSearcherAvailable(),
                    c.getDocCount(), c.getSegmentCount());
        }
    }

    // ── 3. Multi-Tenancy ────────────────────────────────────────────────────

    private static void multiTenancy(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub)
            throws Exception {
        banner("3. Multi-Tenancy — Indexing");

        indexDocs(stub, ACME_DOCS, "acme");
        indexDocs(stub, GLOBEX_DOCS, "globex");

        Thread.sleep(1000); // NRT refresh

        banner("3b. Multi-Tenancy — Tenant-Scoped Queries");

        for (String tenant : List.of("acme", "globex")) {
            CountResponse count = stub.count(CountRequest.newBuilder()
                    .setCollection(COLLECTION).setTenantId(tenant).build());
            System.out.printf("  Tenant '%s' doc count: %d%n", tenant, count.getCount());
        }

        QueryResponse qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION).setTextQuery("platform")
                .setTenantId("acme").setTopK(10).setIncludeSource(true).build());
        System.out.printf("%n  Query 'platform' scoped to 'acme' → %d hit(s):%n",
                qr.getHitsCount());
        for (QueryHit h : qr.getHitsList()) {
            var doc = MAPPER.readTree(h.getSource());
            System.out.printf("    %s: %s%n",
                    doc.get("id").asText(), doc.get("title").asText());
        }

        qr = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION).setTextQuery("platform")
                .setTenantId("globex").setTopK(10).setIncludeSource(true).build());
        System.out.printf("%n  Query 'platform' scoped to 'globex' → %d hit(s) (expected 0)%n",
                qr.getHitsCount());
    }

    // ── 4. Delete By Query ──────────────────────────────────────────────────

    private static void deleteByQuery(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub)
            throws Exception {
        banner("4. Delete By Query");

        DeleteByQueryResponse delResp = stub.deleteByQuery(DeleteByQueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setTenantId("acme")
                .addFilters(FilterClause.newBuilder()
                        .setField("category")
                        .setTermFilter(TermFilter.newBuilder().setValue("hardware")))
                .build());
        System.out.printf("  Deleted category='hardware' for tenant 'acme' (count=%d)%n",
                delResp.getDeletedCount());

        Thread.sleep(1000);

        CountResponse count = stub.count(CountRequest.newBuilder()
                .setCollection(COLLECTION).setTenantId("acme").build());
        System.out.printf("  Tenant 'acme' doc count after delete: %d%n", count.getCount());
    }

    // ── 5. Force Merge ──────────────────────────────────────────────────────

    private static void forceMerge(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("5. Force Merge");

        ForceMergeResponse mergeResp = stub.forceMerge(ForceMergeRequest.newBuilder()
                .setCollection(COLLECTION).setMaxSegments(1).build());
        System.out.printf("  Segments before: %d%n", mergeResp.getSegmentsBefore());
        System.out.printf("  Segments after : %d%n", mergeResp.getSegmentsAfter());
    }

    // ── Cleanup ─────────────────────────────────────────────────────────────

    private static void cleanup(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("Cleanup");
        stub.closeCollection(CloseCollectionRequest.newBuilder()
                .setCollection(COLLECTION).build());
        System.out.printf("  ✓ Closed '%s'%n", COLLECTION);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static void indexDocs(
            JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub,
            List<Map<String, String>> docs, String tenant) throws Exception {
        IndexRequest.Builder req = IndexRequest.newBuilder().setCollection(COLLECTION);
        for (Map<String, String> doc : docs) {
            req.addItem(IndexItem.newBuilder()
                    .setDocument(MAPPER.writeValueAsString(doc))
                    .setTenantId(tenant)
                    .build());
        }
        stub.index(req.build());
        System.out.printf("  Indexed %d docs for tenant '%s'%n", docs.size(), tenant);
    }

    private static void banner(String msg) {
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.printf("  %s%n", msg);
        System.out.println("=".repeat(60));
    }
}
