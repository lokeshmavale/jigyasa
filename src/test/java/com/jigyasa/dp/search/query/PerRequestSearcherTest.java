package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PerRequestSearcherTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSearcher baseSearcher;
    private IndexSearcherManager mockManager;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        for (int i = 0; i < 5; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc" + i, Field.Store.YES));
            writer.addDocument(doc);
        }
        writer.commit();
        baseSearcher = new IndexSearcher(DirectoryReader.open(directory));

        mockManager = mock(IndexSearcherManager.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    private IndexSearcherManager.SearcherLease lease() {
        return new IndexSearcherManager.SearcherLease(baseSearcher, mockManager);
    }

    @Test
    @DisplayName("Default timeout (0) resolves to 30s — query completes normally")
    void defaultTimeoutResolves() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), 0)) {
            TopDocs results = prs.searcher().search(new MatchAllDocsQuery(), 10);
            assertThat(results.totalHits.value()).isEqualTo(5);
            assertThat(prs.timedOut()).isFalse();
        }
    }

    @Test
    @DisplayName("Positive timeout — query completes within deadline")
    void positiveTimeoutCompletesNormally() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), 5000)) {
            TopDocs results = prs.searcher().search(new MatchAllDocsQuery(), 10);
            assertThat(results.totalHits.value()).isEqualTo(5);
            assertThat(prs.timedOut()).isFalse();
        }
    }

    @Test
    @DisplayName("Negative timeout — no timeout set, query runs unlimited")
    void negativeTimeoutMeansNoLimit() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), -1)) {
            TopDocs results = prs.searcher().search(new MatchAllDocsQuery(), 10);
            assertThat(results.totalHits.value()).isEqualTo(5);
            assertThat(prs.timedOut()).isFalse();
        }
    }

    @Test
    @DisplayName("Per-request searcher shares IndexReader with original")
    void sharesIndexReader() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), 0)) {
            assertThat(prs.searcher().getIndexReader())
                    .isSameAs(baseSearcher.getIndexReader());
        }
    }

    @Test
    @DisplayName("Per-request searcher shares similarity with original")
    void sharesSimilarity() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), 0)) {
            assertThat(prs.searcher().getSimilarity())
                    .isEqualTo(baseSearcher.getSimilarity());
        }
    }

    @Test
    @DisplayName("Per-request searcher is distinct instance from original")
    void distinctInstance() throws Exception {
        try (var prs = new PerRequestSearcher(lease(), 100)) {
            assertThat(prs.searcher()).isNotSameAs(baseSearcher);
        }
    }

    @Test
    @DisplayName("Two concurrent per-request searchers have isolated timeout state")
    void isolatedTimeoutState() throws Exception {
        try (var prs1 = new PerRequestSearcher(lease(), 100);
             var prs2 = new PerRequestSearcher(lease(), 60_000)) {
            // They share the same IndexReader but are different searcher instances
            assertThat(prs1.searcher()).isNotSameAs(prs2.searcher());
            assertThat(prs1.searcher().getIndexReader())
                    .isSameAs(prs2.searcher().getIndexReader());
        }
    }

    @Test
    @DisplayName("AutoCloseable — close() does not throw")
    void closeIsIdempotent() throws Exception {
        var prs = new PerRequestSearcher(lease(), 100);
        prs.close();
        prs.close(); // double close is safe
    }
}
