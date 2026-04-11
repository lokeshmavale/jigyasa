package com.jigyasa.dp.search.models;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * HNSW vector index configuration.
 * Controls the quality/speed tradeoff for KNN vector search.
 *
 * Defaults match Lucene 10.4 defaults for balanced performance.
 */
@Getter
@Setter
public class HnswConfig {
    /** Max connections per node in the HNSW graph. Higher = better recall, more memory. */
    public static final int DEF_MAX_CONN = 16;
    /** Beam width during indexing. Higher = better graph quality, slower indexing. */
    public static final int DEF_BEAM_WIDTH = 100;

    private Integer maxConn;
    private Integer beamWidth;
    /** If true, use scalar quantization (int8) to reduce memory ~4x. */
    private Boolean scalarQuantization;

    public int getMaxConn() {
        return Objects.requireNonNullElse(maxConn, DEF_MAX_CONN);
    }

    public int getBeamWidth() {
        return Objects.requireNonNullElse(beamWidth, DEF_BEAM_WIDTH);
    }

    public boolean isScalarQuantization() {
        return Objects.requireNonNullElse(scalarQuantization, false);
    }
}
