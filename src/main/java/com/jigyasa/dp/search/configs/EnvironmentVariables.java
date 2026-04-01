package com.jigyasa.dp.search.configs;

import org.apache.commons.lang3.StringUtils;

/**
 * Contains all the external configs sent to backend via environmental variable
 */
public class EnvironmentVariables {

    public static final EnvironmentalVariable INDEX_SCHEMA_PATH = new EnvironmentalVariable("INDEX_SCHEMA_PATH", null);
    public static EnvironmentalVariable GRPC_SERVER_PORT = new EnvironmentalVariable("GRPC_SERVER_PORT", "50051");
    public static EnvironmentalVariable INDEX_CACHE_DIR = new EnvironmentalVariable("INDEX_CACHE_DIR", "./IndexData/");
    public static EnvironmentalVariable SERVER_MODE = new EnvironmentalVariable("SERVER_MODE", "READ_WRITE");
    public static EnvironmentalVariable DOCID_OVERLAP_TIMEOUT_MS = new EnvironmentalVariable("DOCID_OVERLAP_TIMEOUT_MS", "30000");
    public static EnvironmentalVariable MAX_VECTOR_DIMENSION = new EnvironmentalVariable("MAX_VECTOR_DIMENSION", "2048");
    public static EnvironmentalVariable TRANSLOG_DIRECTORY = new EnvironmentalVariable("TRANSLOG_DIRECTORY", "/TransLog/");
    public static EnvironmentalVariable RAM_BUFFER_SIZE_MB = new EnvironmentalVariable("RAM_BUFFER_SIZE_MB", "256");
    public static EnvironmentalVariable USE_COMPOUND_FILE = new EnvironmentalVariable("USE_COMPOUND_FILE", "false");
    public static EnvironmentalVariable MERGE_MAX_THREADS = new EnvironmentalVariable("MERGE_MAX_THREADS", "2");
    public static EnvironmentalVariable MERGE_MAX_MERGE_COUNT = new EnvironmentalVariable("MERGE_MAX_MERGE_COUNT", "4");
    // Translog durability: "request" = fsync per request (safest), "async" = periodic fsync (fastest)
    public static EnvironmentalVariable TRANSLOG_DURABILITY = new EnvironmentalVariable("TRANSLOG_DURABILITY", "request");
    // Async flush interval in ms (only used when TRANSLOG_DURABILITY=async)
    public static EnvironmentalVariable TRANSLOG_FLUSH_INTERVAL_MS = new EnvironmentalVariable("TRANSLOG_FLUSH_INTERVAL_MS", "200");

    public record EnvironmentalVariable(String name, String defVal) {

        public String get() {
            return System.getenv(name);
        }

        public String defaultIfEmpty() {
            return StringUtils.defaultIfEmpty(get(), defVal);
        }
    }
}
