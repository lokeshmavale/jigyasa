package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.protocol.IndexItem;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;

/**
 * Injects system-level fields into every indexed document.
 * These fields are not part of the user schema — they are infrastructure fields
 * for memory tier management, TTL expiry, and temporal queries.
 *
 * System fields:
 *   _memory_tier   — keyword, filterable (WORKING/EPISODIC/SEMANTIC)
 *   _ttl_expires_at — long, filterable (epoch ms, 0 = never expires)
 *   _indexed_at    — long, filterable + sortable (epoch ms)
 */
public final class SystemFields {

    public static final String MEMORY_TIER = "_memory_tier";
    public static final String TTL_EXPIRES_AT = "_ttl_expires_at";
    public static final String INDEXED_AT = "_indexed_at";
    public static final String TENANT_ID = "_tenant_id";

    // Default TTLs per tier (seconds)
    public static final int DEFAULT_TTL_WORKING_SECS = 300;       // 5 minutes
    public static final int DEFAULT_TTL_EPISODIC_SECS = 86400;    // 24 hours
    public static final int DEFAULT_TTL_SEMANTIC_SECS = 0;        // never

    private SystemFields() {}

    /**
     * Adds system fields to a Lucene document based on the IndexItem metadata.
     */
    public static void addSystemFields(Document doc, IndexItem item) {
        long now = System.currentTimeMillis();

        // Memory tier
        String tier = item.getMemoryTier().name();
        doc.add(new StringField(MEMORY_TIER, tier, Field.Store.YES));
        doc.add(new SortedDocValuesField(MEMORY_TIER, new org.apache.lucene.util.BytesRef(tier)));

        // TTL expiry — LongPoint for range queries, StoredField for retrieval
        int ttlSecs = resolveTtlSeconds(item);
        long expiresAt = (ttlSecs > 0) ? now + (ttlSecs * 1000L) : 0L;
        doc.add(new LongPoint(TTL_EXPIRES_AT, expiresAt));
        doc.add(new StoredField(TTL_EXPIRES_AT, expiresAt));

        // Indexed timestamp — LongPoint for range, NumericDocValues for recency decay, SortedNumeric for sort
        doc.add(new LongPoint(INDEXED_AT, now));
        doc.add(new NumericDocValuesField(INDEXED_AT, now));
        doc.add(new SortedNumericDocValuesField(INDEXED_AT + "$o", now));
        doc.add(new StoredField(INDEXED_AT, now));

        // Tenant ID (only if provided)
        if (!item.getTenantId().isEmpty()) {
            doc.add(new StringField(TENANT_ID, item.getTenantId(), Field.Store.YES));
            doc.add(new SortedDocValuesField(TENANT_ID, new org.apache.lucene.util.BytesRef(item.getTenantId())));
        }
    }

    /**
     * Resolves effective TTL: custom override > tier default.
     */
    public static int resolveTtlSeconds(IndexItem item) {
        if (item.getTtlSeconds() > 0) {
            return item.getTtlSeconds();
        }
        return switch (item.getMemoryTier()) {
            case WORKING -> DEFAULT_TTL_WORKING_SECS;
            case EPISODIC -> DEFAULT_TTL_EPISODIC_SECS;
            case SEMANTIC -> DEFAULT_TTL_SEMANTIC_SECS;
            default -> DEFAULT_TTL_SEMANTIC_SECS;
        };
    }
}
