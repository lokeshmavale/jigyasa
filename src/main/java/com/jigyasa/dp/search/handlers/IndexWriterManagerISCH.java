package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.codecs.CustomLucene99AnweshanCodec;
import com.jigyasa.dp.search.models.IndexSchema;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@RequiredArgsConstructor
public class IndexWriterManagerISCH implements IndexSchemaChangeHandler {
    private final String indexCacheDirectory;

    private volatile IndexWriter indexWriter;
    private final ReentrantReadWriteLock writerAccessLock = new ReentrantReadWriteLock(true);
    private IndexSchema indexSchema;


    @Override
    public void handle(IndexSchema newIndexSchema, IndexSchema oldIndexSchema) {
        updateIndexWriter(newIndexSchema);
        this.indexSchema = newIndexSchema;
    }

    void updateIndexWriter(IndexSchema newIndexSchema) {
        writerAccessLock.writeLock().lock();
        try {
            this.indexWriter = initWriter(newIndexSchema);
        } finally {
            writerAccessLock.writeLock().unlock();
        }
    }

    //Todo: remove sneaky throws
    @SneakyThrows
    private IndexWriter initWriter(IndexSchema newIndexSchema) {
        IndexWriterConfig config = new IndexWriterConfig(newIndexSchema.getInitializedSchema().getIndexAnalyzer());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        config.setUseCompoundFile(true);
        config.setCodec(new CustomLucene99AnweshanCodec(newIndexSchema));
        config.setSimilarity(newIndexSchema.getInitializedSchema().getBm25Similarity());
        Directory directory = FSDirectory.open(Path.of(indexCacheDirectory));
        return new IndexWriter(directory, config);
    }

    public IndexWriter acquireWriter() {
        if (!indexWriter.isOpen()) {
            updateIndexWriter(indexSchema);
        }
        writerAccessLock.readLock().lock();
        return indexWriter;
    }

    public void releaseWriter() {
        writerAccessLock.readLock().unlock();
    }



}
