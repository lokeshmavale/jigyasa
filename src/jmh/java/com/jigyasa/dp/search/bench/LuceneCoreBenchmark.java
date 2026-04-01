package com.jigyasa.dp.search.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmarks for raw Lucene operations — no gRPC, no transport overhead.
 * This measures the absolute floor of what Jigyasa can do.
 *
 * Run: ./gradlew jmh
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class LuceneCoreBenchmark {

    private ByteBuffersDirectory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private StandardAnalyzer analyzer;
    private QueryParser messageParser;

    private static final String[] CATEGORIES = {"web", "api", "static", "auth", "data", "admin"};
    private static final String[] METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"};
    private static final String[] SEARCH_TERMS = {
        "Lucene search engine", "machine learning", "distributed systems",
        "vector search", "Docker container", "gRPC protocol"
    };
    private static final String[] TEXT_CORPUS = {
        "Apache Lucene is a high-performance full-text search engine library written in Java",
        "Elasticsearch is a distributed search and analytics engine built on Apache Lucene",
        "Vector search using HNSW graphs provides approximate nearest neighbor capabilities",
        "Machine learning models require efficient storage and retrieval of training data",
        "Kubernetes orchestrates containerized applications across a cluster of machines",
        "Microservices architecture enables independent deployment and scaling of services",
        "Natural language processing enables machines to understand human language",
        "Transformer models like BERT and GPT have revolutionized NLP tasks",
        "Distributed systems require careful handling of network partitions and consistency",
        "Agent memory systems need persistent, searchable, and rankable storage",
    };

    private Random random;
    private int docCount;

    @Param({"10000", "100000", "1000000"})
    private int numDocs;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        random = new Random(42);
        analyzer = new StandardAnalyzer();
        directory = new ByteBuffersDirectory();

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setRAMBufferSizeMB(256.0);
        config.setUseCompoundFile(false);
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setSegmentsPerTier(10.0);
        config.setMergePolicy(mergePolicy);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                doc.add(new StringField("method", METHODS[random.nextInt(METHODS.length)], Field.Store.YES));
                doc.add(new StringField("category", CATEGORIES[random.nextInt(CATEGORIES.length)], Field.Store.YES));
                doc.add(new IntPoint("status", 200 + random.nextInt(400)));
                doc.add(new NumericDocValuesField("status_dv", 200 + random.nextInt(400)));
                doc.add(new IntPoint("response_time_ms", random.nextInt(5000)));
                doc.add(new NumericDocValuesField("response_time_ms_dv", random.nextInt(5000)));
                String msg = TEXT_CORPUS[random.nextInt(TEXT_CORPUS.length)] + " "
                           + TEXT_CORPUS[random.nextInt(TEXT_CORPUS.length)];
                doc.add(new TextField("message", msg, Field.Store.YES));
                doc.add(new StoredField("_source", "{\"id\":\"doc-" + i + "\"}"));
                writer.addDocument(doc);
            }
            writer.forceMerge(5);
        }

        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        messageParser = new QueryParser("message", analyzer);
        docCount = reader.numDocs();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    // ---- Benchmarks ----

    @Benchmark
    public TopDocs bm25TextSearch() throws Exception {
        String term = SEARCH_TERMS[random.nextInt(SEARCH_TERMS.length)];
        Query query = messageParser.parse(term);
        return searcher.search(query, 10);
    }

    @Benchmark
    public TopDocs termFilter() throws Exception {
        String cat = CATEGORIES[random.nextInt(CATEGORIES.length)];
        Query query = new TermQuery(new Term("category", cat));
        return searcher.search(query, 10);
    }

    @Benchmark
    public TopDocs rangeFilter() throws Exception {
        Query query = IntPoint.newRangeQuery("status", 400, 503);
        return searcher.search(query, 10);
    }

    @Benchmark
    public TopDocs boolCompound() throws Exception {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("method", "GET")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("category", "api")), BooleanClause.Occur.SHOULD);
        builder.add(new TermQuery(new Term("category", "web")), BooleanClause.Occur.SHOULD);
        return searcher.search(builder.build(), 10);
    }

    @Benchmark
    public TopDocs textPlusFilter() throws Exception {
        String term = SEARCH_TERMS[random.nextInt(SEARCH_TERMS.length)];
        Query textQuery = messageParser.parse(term);
        Query rangeQuery = IntPoint.newRangeQuery("status", 200, 299);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(textQuery, BooleanClause.Occur.MUST);
        builder.add(rangeQuery, BooleanClause.Occur.FILTER);
        return searcher.search(builder.build(), 10);
    }

    @Benchmark
    public TopFieldDocs sortByField() throws Exception {
        Sort sort = new Sort(new SortField("response_time_ms_dv", SortField.Type.LONG, true));
        return searcher.search(new MatchAllDocsQuery(), 10, sort);
    }

    @Benchmark
    public int countAll() throws Exception {
        return searcher.count(new MatchAllDocsQuery());
    }

    @Benchmark
    public int countWithFilter() throws Exception {
        return searcher.count(IntPoint.newRangeQuery("status", 400, 503));
    }
}
