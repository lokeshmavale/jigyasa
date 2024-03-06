package com.jigyasa.dp.search.models;

import lombok.Data;
import org.apache.lucene.index.VectorSimilarityFunction;


@Data
public class HNSWConfig {
    private String name;
    private VectorSimilarityFunction distanceMetric;
    private final int m;
    private final int efConstruction;
    private final int efSearch;
}
