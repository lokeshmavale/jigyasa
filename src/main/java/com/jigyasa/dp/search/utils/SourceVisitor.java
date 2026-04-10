package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

public class SourceVisitor extends StoredFieldVisitor {
    private byte[] src;

    /** Returns the raw source bytes WITHOUT cloning. Caller must not mutate. */
    public byte[] getSrc() {
        return src;
    }

    /** Reset for reuse across hits in the same query. */
    public void reset() {
        this.src = null;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        this.src = value;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        return FieldMapperStrategy.SOURCE_FIELD_NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
    }
}
