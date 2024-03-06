package com.jigyasa.dp.search.models;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class IndexSchema {

    private SchemaField[] fields;
    private BM25Config bm25Config;
    private HNSWConfig[] HNSWConfigs;
    private transient InitializedIndexSchema initializedSchema;

    public BM25Config getBm25Config() {
        return Objects.requireNonNullElseGet(bm25Config, BM25Config::new);
    }
}
