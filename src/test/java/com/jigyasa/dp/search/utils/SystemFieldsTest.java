package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.MemoryTier;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SystemFieldsTest {

    @Test
    void addSystemFields_semanticTier_noExpiry() {
        Document doc = new Document();
        IndexItem item = IndexItem.newBuilder()
                .setMemoryTier(MemoryTier.SEMANTIC)
                .build();

        SystemFields.addSystemFields(doc, item);

        assertThat(doc.get(SystemFields.MEMORY_TIER)).isEqualTo("SEMANTIC");
        assertThat(doc.getField(SystemFields.TTL_EXPIRES_AT)).isNotNull();
        assertThat(doc.getField(SystemFields.INDEXED_AT)).isNotNull();
    }

    @Test
    void addSystemFields_workingTier_defaultTtl5min() {
        Document doc = new Document();
        IndexItem item = IndexItem.newBuilder()
                .setMemoryTier(MemoryTier.WORKING)
                .build();

        long before = System.currentTimeMillis();
        SystemFields.addSystemFields(doc, item);
        long after = System.currentTimeMillis();

        assertThat(doc.get(SystemFields.MEMORY_TIER)).isEqualTo("WORKING");
        long expectedMin = before + (SystemFields.DEFAULT_TTL_WORKING_SECS * 1000L);
        long expectedMax = after + (SystemFields.DEFAULT_TTL_WORKING_SECS * 1000L);
        String storedTtl = doc.get(SystemFields.TTL_EXPIRES_AT);
        assertThat(Long.parseLong(storedTtl)).isBetween(expectedMin, expectedMax);
    }

    @Test
    void addSystemFields_episodicTier_defaultTtl24h() {
        int resolved = SystemFields.resolveTtlSeconds(
                IndexItem.newBuilder().setMemoryTier(MemoryTier.EPISODIC).build());
        assertThat(resolved).isEqualTo(86400);
    }

    @Test
    void addSystemFields_customTtlOverridesTierDefault() {
        IndexItem item = IndexItem.newBuilder()
                .setMemoryTier(MemoryTier.WORKING)
                .setTtlSeconds(60)
                .build();
        assertThat(SystemFields.resolveTtlSeconds(item)).isEqualTo(60);
    }

    @Test
    void addSystemFields_defaultTierIsSemantic() {
        IndexItem item = IndexItem.newBuilder().build();
        assertThat(item.getMemoryTier()).isEqualTo(MemoryTier.SEMANTIC);
        assertThat(SystemFields.resolveTtlSeconds(item)).isEqualTo(0);
    }

    @Test
    void addSystemFields_indexedAtIsReasonable() {
        Document doc = new Document();
        IndexItem item = IndexItem.newBuilder().build();

        long before = System.currentTimeMillis();
        SystemFields.addSystemFields(doc, item);
        long after = System.currentTimeMillis();

        String storedAt = doc.get(SystemFields.INDEXED_AT);
        assertThat(Long.parseLong(storedAt)).isBetween(before, after);
    }
}
