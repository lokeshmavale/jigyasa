package com.jigyasa.dp.search.models;

import lombok.Data;

@Data
public class SchemaField {
    private String name;
    private FieldDataType type;
    private String searchAnalyzer;
    private String indexAnalyzer;
    private Integer dimension;
    private String similarityFunction; // COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT

    //Field Properties
    private boolean key;
    private boolean searchable;
    private boolean filterable;
    private boolean sortable;
    private boolean facetable;
}
