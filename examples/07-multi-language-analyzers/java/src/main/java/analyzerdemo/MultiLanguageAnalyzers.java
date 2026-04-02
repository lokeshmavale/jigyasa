package analyzerdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.jigyasa.dp.search.protocol.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Example 07 — Multi-Language Analyzers (Java)
 *
 * Demonstrates per-field analyzer configuration for multilingual search:
 *   1. Schema with indexAnalyzer / searchAnalyzer per field
 *   2. French, German, Hindi, CJK, English stemming vs standard analyzer
 *   3. Keyword analyzer for exact-match fields
 */
public class MultiLanguageAnalyzers {

    private static final String ADDR = "localhost:50051";
    private static final String COLLECTION = "analyzers_demo_java";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Multilingual test corpus
    record Doc(String id, String lang, String title, String body) {}

    static final List<Doc> DOCS = List.of(
        // French
        new Doc("fr1", "fr", "Les maisons blanches sont belles",
                "Les architectes français construisent de magnifiques maisons blanches dans le sud de la France."),
        new Doc("fr2", "fr", "Architecture française moderne",
                "La construction de bâtiments modernes utilise des matériaux écologiques et durables."),
        // German
        new Doc("de1", "de", "Häuser und Gebäude in Deutschland",
                "Die deutschen Architekten entwerfen energieeffiziente Wohnhäuser und Bürogebäude."),
        new Doc("de2", "de", "Moderne Stadtentwicklung",
                "Städtebauliche Entwicklung in Berlin umfasst nachhaltige Gebäude und grüne Infrastruktur."),
        // Hindi
        new Doc("hi1", "hi", "भारतीय वास्तुकला की सुंदरता",
                "भारतीय मंदिरों की वास्तुकला विश्व प्रसिद्ध है। पुराने मंदिर अद्भुत शिल्पकला के नमूने हैं।"),
        new Doc("hi2", "hi", "आधुनिक भवन निर्माण",
                "भारत में आधुनिक भवनों का निर्माण तेजी से हो रहा है। नई तकनीकें इमारतों को मजबूत बनाती हैं।"),
        // CJK (Japanese)
        new Doc("ja1", "cjk", "日本の伝統的な建築",
                "日本の伝統的な建築様式は、木造建築を基本としています。寺院や神社は美しい建築物です。"),
        new Doc("ja2", "cjk", "東京の現代建築",
                "東京には世界的に有名な現代建築が数多くあります。高層ビルやタワーが都市の景観を形成しています。"),
        // English
        new Doc("en1", "en", "Modern Architecture Trends",
                "Architects are designing sustainable buildings using renewable materials and energy-efficient systems."),
        new Doc("en2", "en", "Historical Buildings of Europe",
                "European cathedrals and castles represent centuries of architectural innovation and craftsmanship.")
    );

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(ADDR).usePlaintext().build();
        JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub =
                JigyasaDataPlaneServiceGrpc.newBlockingStub(channel);

        try {
            createCollection(stub);
            indexDocuments(stub);
            Thread.sleep(1000); // NRT refresh

            demoFrenchStemming(stub);
            demoGermanStemming(stub);
            demoHindiSearch(stub);
            demoCjkSearch(stub);
            demoEnglishStemming(stub);
            demoKeywordAnalyzer(stub);
            demoCrossLanguageFilter(stub);

            banner("✅ Multi-language analyzer demo complete!");
            System.out.println("  Supported: standard, simple, keyword, whitespace,");
            System.out.println("  + 38 language analyzers: lucene.en, lucene.fr, lucene.de,");
            System.out.println("    lucene.hi, lucene.cjk, lucene.ar, lucene.ru, lucene.es, ...");
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // ── Schema with per-field analyzers ──────────────────────────────────────

    static void createCollection(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) throws Exception {
        banner("1. Create collection with per-field analyzers");

        Map<String, Object> schema = Map.of("fields", List.of(
            field("id",        "STRING", true, false, true, false, null, null),
            field("lang",      "STRING", false, false, true, false, null, null),
            field("title_std", "STRING", false, true, false, false, "standard",  "standard"),
            field("title_fr",  "STRING", false, true, false, false, "lucene.fr", "lucene.fr"),
            field("title_de",  "STRING", false, true, false, false, "lucene.de", "lucene.de"),
            field("title_hi",  "STRING", false, true, false, false, "lucene.hi", "lucene.hi"),
            field("title_cjk", "STRING", false, true, false, false, "lucene.cjk","lucene.cjk"),
            field("title_en",  "STRING", false, true, false, false, "lucene.en", "lucene.en"),
            field("body_std",  "STRING", false, true, false, false, "standard",  "standard"),
            field("tag",       "STRING", false, true, false, false, "keyword",   "keyword")
        ));

        String schemaJson = mapper.writeValueAsString(schema);
        try {
            stub.createCollection(CreateCollectionRequest.newBuilder()
                    .setCollection(COLLECTION).setIndexSchema(schemaJson).build());
            System.out.println("  ✓ Created collection '" + COLLECTION + "'");
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.println("  ✓ Collection already exists — reusing");
            } else {
                throw e;
            }
        }
    }

    static Map<String, Object> field(String name, String type,
                                      boolean key, boolean searchable,
                                      boolean filterable, boolean sortable,
                                      String indexAnalyzer, String searchAnalyzer) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("name", name);
        f.put("type", type);
        if (key) f.put("key", true);
        if (searchable) f.put("searchable", true);
        if (filterable) f.put("filterable", true);
        if (sortable) f.put("sortable", true);
        if (indexAnalyzer != null) f.put("indexAnalyzer", indexAnalyzer);
        if (searchAnalyzer != null) f.put("searchAnalyzer", searchAnalyzer);
        return f;
    }

    // ── Index documents ─────────────────────────────────────────────────────

    static void indexDocuments(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) throws Exception {
        banner("2. Index multilingual documents");

        List<IndexItem> items = new ArrayList<>();
        for (Doc doc : DOCS) {
            ObjectNode node = mapper.createObjectNode();
            node.put("id", doc.id());
            node.put("lang", doc.lang());
            node.put("title_std", doc.title());
            node.put("title_" + doc.lang(), doc.title());
            node.put("body_std", doc.body());
            node.put("tag", "architecture-" + doc.lang());
            items.add(IndexItem.newBuilder()
                    .setDocument(mapper.writeValueAsString(node))
                    .build());
        }

        IndexResponse resp = stub.index(IndexRequest.newBuilder()
                .addAllItem(items).setCollection(COLLECTION).build());
        long ok = resp.getItemResponseList().stream().filter(s -> s.getCode() == 0).count();
        System.out.printf("  ✓ Indexed %d/%d documents%n", ok, DOCS.size());
    }

    // ── Query helpers ───────────────────────────────────────────────────────

    static void printHits(QueryResponse resp, String label) {
        System.out.println("  " + label);
        System.out.printf("  Hits: %d%n", resp.getTotalHits());
        for (int i = 0; i < resp.getHitsCount(); i++) {
            QueryHit hit = resp.getHits(i);
            try {
                var doc = mapper.readTree(hit.getSource());
                String title = Optional.ofNullable(doc.get("title_std"))
                        .map(n -> n.asText()).orElse(doc.get("id").asText());
                String lang = doc.get("lang").asText();
                System.out.printf("    %d. [%.4f] [%s] %s%n", i + 1, hit.getScore(), lang, title);
            } catch (Exception e) {
                System.out.printf("    %d. [%.4f] (parse error)%n", i + 1, hit.getScore());
            }
        }
        if (resp.getTotalHits() == 0) System.out.println("    (none)");
        System.out.println();
    }

    static QueryResponse queryField(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub,
                                     String field, String query) {
        return stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setQueryString(field + ":" + query)
                .setTopK(5)
                .setIncludeSource(true)
                .build());
    }

    // ── Demo methods ────────────────────────────────────────────────────────

    static void demoFrenchStemming(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("3. French stemming: 'maison' (singular) → finds 'maisons' (plural)");
        printHits(queryField(stub, "title_std", "maison"), "Standard analyzer on 'maison':");
        printHits(queryField(stub, "title_fr", "maison"),  "French analyzer (lucene.fr) on 'maison':");
    }

    static void demoGermanStemming(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("4. German stemming: 'Haus' → finds 'Häuser' (plural with umlaut)");
        printHits(queryField(stub, "title_std", "Haus"), "Standard analyzer on 'Haus':");
        printHits(queryField(stub, "title_de", "Haus"),  "German analyzer (lucene.de) on 'Haus':");
    }

    static void demoHindiSearch(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("5. Hindi search: 'वास्तुकला' (architecture)");
        printHits(queryField(stub, "title_hi", "वास्तुकला"), "Hindi analyzer (lucene.hi) on 'वास्तुकला':");
    }

    static void demoCjkSearch(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("6. CJK search: '建築' (architecture in Japanese)");
        printHits(queryField(stub, "title_cjk", "建築"), "CJK analyzer (lucene.cjk) on '建築':");
    }

    static void demoEnglishStemming(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("7. English stemming: 'architectural' → finds 'architecture/architects'");
        printHits(queryField(stub, "title_std", "architectural"), "Standard analyzer on 'architectural':");
        printHits(queryField(stub, "title_en", "architectural"),  "English analyzer (lucene.en) on 'architectural':");
    }

    static void demoKeywordAnalyzer(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("8. Keyword analyzer: exact-match tag search");
        printHits(queryField(stub, "tag", "architecture"),    "Keyword field, query='architecture' (partial — no match expected):");
        printHits(queryField(stub, "tag", "architecture-fr"), "Keyword field, query='architecture-fr' (exact match):");
    }

    static void demoCrossLanguageFilter(JigyasaDataPlaneServiceGrpc.JigyasaDataPlaneServiceBlockingStub stub) {
        banner("9. Cross-language: French text search + lang filter");
        QueryResponse resp = stub.query(QueryRequest.newBuilder()
                .setCollection(COLLECTION)
                .setQueryString("body_std:construction AND body_std:bâtiment")
                .setTopK(5)
                .setIncludeSource(true)
                .addFilters(FilterClause.newBuilder()
                        .setField("lang")
                        .setTermFilter(TermFilter.newBuilder().setValue("fr").build())
                        .build())
                .build());
        printHits(resp, "Body search 'construction bâtiment' filtered to lang=fr:");
    }

    static void banner(String text) {
        System.out.println();
        System.out.println("=".repeat(64));
        System.out.println("  " + text);
        System.out.println("=".repeat(64));
    }
}
