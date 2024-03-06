package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import lombok.Getter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

public class SourceVisitor extends StoredFieldVisitor {
    @Getter
    byte[] src;

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        this.src = value;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        return FieldMapperStrategy.SOURCE_FEILD_NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
    }
}
