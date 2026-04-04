package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.configs.EnvironmentVariables;
import com.jigyasa.dp.search.models.HnswConfig;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.utils.SchemaUtil;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@RequiredArgsConstructor
public class IndexWriterManagerISCH implements IndexSchemaChangeHandler {
    private static final Logger log = LoggerFactory.getLogger(IndexWriterManagerISCH.class);
    public static final String COMMIT_DATA_SCHEMA_KEY = "_schema_json";

    private final String indexCacheDirectory;

    private volatile IndexWriter indexWriter;
    private volatile Directory directory;
    private final ReentrantReadWriteLock writerAccessLock = new ReentrantReadWriteLock(true);
    private volatile IndexSchema indexSchema;
    private volatile boolean closed = false;

    /**
     * Returns the current IndexWriter reference without locking.
     * Only safe to call after handle() has completed (writer is initialized).
     * Used by ControlledRealTimeReopenThread which needs the writer reference at construction time.
     */
    public IndexWriter getWriter() {
        return indexWriter;
    }


    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        updateIndexWriter(newIndexSchema);
        this.indexSchema = newIndexSchema;
        // Persist schema JSON into Lucene commit data so it survives restarts
        persistSchemaToCommitData(newIndexSchema);
    }

    /**
     * Sets the schema JSON as live commit data on the IndexWriter.
     * This is written to the index on the next commit() call.
     */
    private void persistSchemaToCommitData(IndexSchema schema) {
        if (indexWriter != null && indexWriter.isOpen()) {
            String schemaJson = SchemaUtil.toJson(schema);
            indexWriter.setLiveCommitData(
                    Map.of(COMMIT_DATA_SCHEMA_KEY, schemaJson).entrySet());
            log.info("Schema persisted to commit data ({} bytes)", schemaJson.length());
        }
    }

    void updateIndexWriter(IndexSchema newIndexSchema) {
        writerAccessLock.writeLock().lock();
        try {
            closeCurrentResources();
            this.indexWriter = initWriter(newIndexSchema);
        } finally {
            writerAccessLock.writeLock().unlock();
        }
    }

    private void closeCurrentResources() {
        if (indexWriter != null && indexWriter.isOpen()) {
            try {
                indexWriter.commit();
                indexWriter.close();
                log.info("Previous IndexWriter committed and closed");
            } catch (IOException e) {
                log.warn("Error closing previous IndexWriter", e);
            }
        }
        if (directory != null) {
            try {
                directory.close();
                log.info("Previous Directory closed");
            } catch (IOException e) {
                log.warn("Error closing previous Directory", e);
            }
        }
    }

    private IndexWriter initWriter(IndexSchema newIndexSchema) {
        try {
            double ramBufferMb = Double.parseDouble(EnvironmentVariables.RAM_BUFFER_SIZE_MB.defaultIfEmpty());
            boolean useCompoundFile = Boolean.parseBoolean(EnvironmentVariables.USE_COMPOUND_FILE.defaultIfEmpty());
            int mergeMaxThreads = Integer.parseInt(EnvironmentVariables.MERGE_MAX_THREADS.defaultIfEmpty());
            int mergeMaxMergeCount = Integer.parseInt(EnvironmentVariables.MERGE_MAX_MERGE_COUNT.defaultIfEmpty());

            IndexWriterConfig config = new IndexWriterConfig(newIndexSchema.getInitializedSchema().getIndexAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            config.setUseCompoundFile(useCompoundFile);
            config.setSimilarity(newIndexSchema.getInitializedSchema().getBm25Similarity());
            config.setRAMBufferSizeMB(ramBufferMb);

            TieredMergePolicy mergePolicy = new TieredMergePolicy();
            mergePolicy.setSegmentsPerTier(10.0);
            config.setMergePolicy(mergePolicy);

            ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
            mergeScheduler.setMaxMergesAndThreads(mergeMaxMergeCount, mergeMaxThreads);
            config.setMergeScheduler(mergeScheduler);

            config.setCodec(buildCodec(newIndexSchema.getHnswConfig()));

            log.info("IndexWriter config: ramBuffer={}MB, compoundFile={}, mergeThreads={}, maxMerges={}",
                    ramBufferMb, useCompoundFile, mergeMaxThreads, mergeMaxMergeCount);

            this.directory = FSDirectory.open(Path.of(indexCacheDirectory));
            return new IndexWriter(directory, config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize IndexWriter at " + indexCacheDirectory, e);
        }
    }

    /**
     * Builds a Lucene codec with HNSW parameters from schema config.
     * Uses FilterCodec to override only the KnnVectorsFormat while keeping
     * all other format handlers from the default Lucene104Codec.
     */
    private static FilterCodec buildCodec(HnswConfig hnsw) {
        int maxConn = hnsw.getMaxConn();
        int beamWidth = hnsw.getBeamWidth();

        KnnVectorsFormat vectorsFormat = hnsw.isScalarQuantization()
                ? new Lucene104HnswScalarQuantizedVectorsFormat(maxConn, beamWidth)
                : new Lucene99HnswVectorsFormat(maxConn, beamWidth);

        log.info("HNSW codec: maxConn={}, beamWidth={}, scalarQuantization={}",
                maxConn, beamWidth, hnsw.isScalarQuantization());

        Lucene104Codec delegate = new Lucene104Codec();
        return new FilterCodec(delegate.getName(), delegate) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return vectorsFormat;
            }
        };
    }

    public IndexWriter acquireWriter() {
        writerAccessLock.readLock().lock();
        if (closed) {
            writerAccessLock.readLock().unlock();
            throw new IllegalStateException("IndexWriterManager has been shut down");
        }
        if (!indexWriter.isOpen()) {
            // Upgrade to write lock to reinitialize
            writerAccessLock.readLock().unlock();
            updateIndexWriter(indexSchema);
            writerAccessLock.readLock().lock();
            // Re-check state after re-acquiring lock (close TOCTOU window)
            if (closed || !indexWriter.isOpen()) {
                writerAccessLock.readLock().unlock();
                throw new IllegalStateException("IndexWriterManager was shut down or writer closed during reinitialization");
            }
        }
        return indexWriter;
    }

    public void releaseWriter() {
        writerAccessLock.readLock().unlock();
    }

    public void shutdown() {
        writerAccessLock.writeLock().lock();
        try {
            closed = true;
            closeCurrentResources();
            log.info("IndexWriterManager shut down");
        } finally {
            writerAccessLock.writeLock().unlock();
        }
    }

    /**
     * Reads the persisted schema JSON from the last Lucene commit in the given directory.
     * Returns null if no index exists or no schema is stored in commit data.
     */
    public static IndexSchema readPersistedSchema(String indexDir) {
        Path path = Path.of(indexDir);
        if (!Files.exists(path)) return null;
        try (Directory dir = FSDirectory.open(path)) {
            if (!DirectoryReader.indexExists(dir)) return null;
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                Map<String, String> commitData = reader.getIndexCommit().getUserData();
                String schemaJson = commitData.get(COMMIT_DATA_SCHEMA_KEY);
                if (schemaJson == null || schemaJson.isEmpty()) return null;
                log.info("Read persisted schema from index at {} ({} bytes)", indexDir, schemaJson.length());
                return SchemaUtil.parseSchema(schemaJson);
            }
        } catch (IOException e) {
            log.warn("Failed to read persisted schema from {}: {}", indexDir, e.getMessage());
            return null;
        }
    }
}
