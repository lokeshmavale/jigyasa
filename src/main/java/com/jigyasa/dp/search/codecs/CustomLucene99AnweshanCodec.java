package com.jigyasa.dp.search.codecs;

import com.jigyasa.dp.search.models.IndexSchema;
import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;

@AutoService(value = Codec.class)
public class CustomLucene99AnweshanCodec extends FilterCodec {
    private IndexSchema schema;

    public CustomLucene99AnweshanCodec(IndexSchema schema) {
        super("CustomLucene99AnweshanCodec", new Lucene99Codec());
        this.schema = schema;
    }

    public CustomLucene99AnweshanCodec() {
        super("CustomLucene99AnweshanCodec", new Lucene99Codec());
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return new CustomLucene99AnweshanHNSWVectorsFormat(schema.getInitializedSchema());
    }

}
