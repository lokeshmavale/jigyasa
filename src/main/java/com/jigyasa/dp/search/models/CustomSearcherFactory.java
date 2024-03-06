package com.jigyasa.dp.search.models;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

@RequiredArgsConstructor
public class CustomSearcherFactory extends SearcherFactory {
    private final Similarity similarity;

    @Override
    public IndexSearcher newSearcher(IndexReader reader, IndexReader prevReader) throws IOException {
        IndexSearcher indexSearcher = super.newSearcher(reader, prevReader);
        indexSearcher.setSimilarity(similarity);
        return indexSearcher;
    }
}
