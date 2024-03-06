package com.jigyasa.dp.search.models;

public enum LuceneFieldType {
    FILTERABLE('f'), SORTABLE('o');

    public static final char FIELD_SPLITTER = '$';
    final char val;

    LuceneFieldType(char val) {
        this.val = val;
    }

    public String toLuceneFieldName(String fieldName) {
        return fieldName + FIELD_SPLITTER + this.val;
    }

    public static String stripFieldTypeExtension(String fieldName) {
        if (fieldName.charAt(fieldName.length() - 2) == FIELD_SPLITTER) {
            return fieldName.substring(0, fieldName.length() - 2);
        }
        return fieldName;
    }

}
