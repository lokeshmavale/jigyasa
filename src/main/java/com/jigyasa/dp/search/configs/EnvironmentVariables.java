package com.jigyasa.dp.search.configs;

import org.apache.commons.lang3.StringUtils;

/**
 * Contains all the external configs sent to backend via environmental variable
 */
public class EnvironmentVariables {

    public static final EnvironmentalVariable INDEX_SCHEMA_PATH = new EnvironmentalVariable("INDEX_SCHEMA_PATH", null);
    public static EnvironmentalVariable GRPC_SERVER_PORT = new EnvironmentalVariable("GRPC_SERVER_PORT", "50051");
    public static EnvironmentalVariable INDEX_CACHE_DIR = new EnvironmentalVariable("INDEX_CACHE_DIR", "./IndexData/");
    public static EnvironmentalVariable SERVER_MODE = new EnvironmentalVariable("SERVER_MODE", "WRITE");
    public static EnvironmentalVariable DOCID_OVERLAP_TIMEOUT_MS = new EnvironmentalVariable("DOCID_OVERLAP_TIMEOUT_MS", "30000");
    public static EnvironmentalVariable MAX_VECTOR_DIMENSION = new EnvironmentalVariable("MAX_VECTOR_DIMENSION", "2048");
    public static EnvironmentalVariable TRANSLOG_DIRECTORY = new EnvironmentalVariable("TRANSLOG_DIRECTORY", "/TransLog/");

    public record EnvironmentalVariable(String name, String defVal) {

        public String get() {
            return System.getenv(name);
        }

        public String defaultIfEmpty() {
            return StringUtils.defaultIfEmpty(get(), defVal);
        }
    }
}
