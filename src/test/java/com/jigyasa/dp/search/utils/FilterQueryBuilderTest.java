package com.jigyasa.dp.search.utils;

import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.CompoundFilter;
import com.jigyasa.dp.search.protocol.ExistsFilter;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.GeoBoundingBoxFilter;
import com.jigyasa.dp.search.protocol.GeoDistanceFilter;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.TermFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for FilterQueryBuilder.
 */
class FilterQueryBuilderTest {

    private InitializedIndexSchema schema;

    @BeforeEach
    void setUp() {
        IndexSchema indexSchema = buildSchema();
        new InitializedSchemaISCH().handle(indexSchema, null);
        schema = indexSchema.getInitializedSchema();
    }

    // =====================================================================
    // Term Filters
    // =====================================================================

    @Test
    @DisplayName("Term filter on STRING produces TermQuery")
    void termFilter_string() {
        Query q = buildFilter(termClause("category", "tech"));
        // BooleanQuery wrapping a TermQuery with FILTER occur
        assertThat(q).isInstanceOf(BooleanQuery.class);
        BooleanQuery bq = (BooleanQuery) q;
        assertThat(bq.clauses()).hasSize(1);
        assertThat(bq.clauses().get(0).query()).isInstanceOf(TermQuery.class);
    }

    @Test
    @DisplayName("Term filter on INT32 produces IntPoint exact query")
    void termFilter_int32() {
        Query q = buildFilter(termClause("quantity", "42"));
        BooleanQuery bq = (BooleanQuery) q;
        // IntPoint.newExactQuery produces an IntPoint range query internally
        Query inner = bq.clauses().get(0).query();
        assertThat(inner.toString()).contains("quantity");
    }

    @Test
    @DisplayName("Term filter on unknown field throws IllegalArgumentException")
    void termFilter_invalidFieldThrows() {
        assertThatThrownBy(() -> buildFilter(termClause("nonexistent", "val")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown filter field");
    }

    @Test
    @DisplayName("Term filter on non-filterable field throws IllegalArgumentException")
    void termFilter_nonFilterableThrows() {
        assertThatThrownBy(() -> buildFilter(termClause("content", "val")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not filterable");
    }

    // =====================================================================
    // Range Filters
    // =====================================================================

    @Test
    @DisplayName("Inclusive range filter produces correct range query")
    void rangeFilter_inclusive() {
        Query q = buildFilter(rangeClause("quantity", "10", "100", false, false));
        BooleanQuery bq = (BooleanQuery) q;
        assertThat(bq.clauses()).hasSize(1);
        // Should match the range [10, 100]
        assertThat(bq.clauses().get(0).query().toString()).contains("quantity");
    }

    @Test
    @DisplayName("Exclusive range filter adjusts boundaries")
    void rangeFilter_exclusive() {
        Query qExcl = buildFilter(rangeClause("quantity", "10", "100", true, true));
        Query qIncl = buildFilter(rangeClause("quantity", "10", "100", false, false));
        // Exclusive should differ from inclusive
        assertThat(qExcl).isNotEqualTo(qIncl);
    }

    @Test
    @DisplayName("Open-ended range with min only")
    void rangeFilter_openEnded_minOnly() {
        Query q = buildFilter(rangeClause("quantity", "50", "", false, false));
        assertThat(q).isNotNull();
        assertThat(q.toString()).contains("quantity");
    }

    @Test
    @DisplayName("Open-ended range with max only")
    void rangeFilter_openEnded_maxOnly() {
        Query q = buildFilter(rangeClause("quantity", "", "50", false, false));
        assertThat(q).isNotNull();
        assertThat(q.toString()).contains("quantity");
    }

    // =====================================================================
    // Geo Filters
    // =====================================================================

    @Test
    @DisplayName("Geo distance filter produces LatLonPoint distance query")
    void geoDistanceFilter_valid() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("location")
                .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                        .setLat(40.7128).setLon(-74.0060).setDistanceMeters(10000))
                .build();
        Query q = buildFilter(fc);
        assertThat(q).isNotNull();
        assertThat(q.toString()).contains("location");
    }

    @Test
    @DisplayName("Geo distance filter with invalid lat throws")
    void geoDistanceFilter_invalidLat() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("location")
                .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                        .setLat(91.0).setLon(0.0).setDistanceMeters(1000))
                .build();
        assertThatThrownBy(() -> buildFilter(fc))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Latitude");
    }

    @Test
    @DisplayName("Geo distance filter with zero distance throws")
    void geoDistanceFilter_zeroDistance() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("location")
                .setGeoDistanceFilter(GeoDistanceFilter.newBuilder()
                        .setLat(40.0).setLon(-74.0).setDistanceMeters(0))
                .build();
        assertThatThrownBy(() -> buildFilter(fc))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    @DisplayName("Geo bounding box filter produces LatLonPoint box query")
    void geoBoundingBox_valid() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("location")
                .setGeoBoundingBoxFilter(GeoBoundingBoxFilter.newBuilder()
                        .setBottomLat(40.0).setTopLat(41.0)
                        .setLeftLon(-75.0).setRightLon(-73.0))
                .build();
        Query q = buildFilter(fc);
        assertThat(q).isNotNull();
    }

    @Test
    @DisplayName("Geo bounding box with inverted lat throws")
    void geoBoundingBox_invertedLat() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("location")
                .setGeoBoundingBoxFilter(GeoBoundingBoxFilter.newBuilder()
                        .setBottomLat(42.0).setTopLat(40.0)
                        .setLeftLon(-75.0).setRightLon(-73.0))
                .build();
        assertThatThrownBy(() -> buildFilter(fc))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bottom latitude must be <= top latitude");
    }

    // =====================================================================
    // Compound Filters
    // =====================================================================

    @Test
    @DisplayName("Compound OR filter uses SHOULD with minShouldMatch(1)")
    void compoundFilter_or() {
        FilterClause compound = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addShould(termClause("category", "tech"))
                        .addShould(termClause("category", "food")))
                .build();
        Query q = buildFilter(compound);
        BooleanQuery outer = (BooleanQuery) q;
        // The outer wrapper has 1 FILTER clause (the compound)
        BooleanQuery inner = (BooleanQuery) outer.clauses().get(0).query();
        long shouldCount = inner.clauses().stream()
                .filter(c -> c.occur() == BooleanClause.Occur.SHOULD).count();
        assertThat(shouldCount).isEqualTo(2);
        assertThat(inner.getMinimumNumberShouldMatch()).isEqualTo(1);
    }

    @Test
    @DisplayName("Compound NOT filter uses MUST_NOT with MatchAll anchor")
    void compoundFilter_not() {
        FilterClause compound = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder()
                        .addMustNot(termClause("category", "tech")))
                .build();
        Query q = buildFilter(compound);
        BooleanQuery outer = (BooleanQuery) q;
        BooleanQuery inner = (BooleanQuery) outer.clauses().get(0).query();

        boolean hasMustAll = inner.clauses().stream()
                .anyMatch(c -> c.occur() == BooleanClause.Occur.MUST && c.query() instanceof MatchAllDocsQuery);
        boolean hasMustNot = inner.clauses().stream()
                .anyMatch(c -> c.occur() == BooleanClause.Occur.MUST_NOT);
        assertThat(hasMustAll).isTrue();
        assertThat(hasMustNot).isTrue();
    }

    @Test
    @DisplayName("Empty compound filter throws IllegalArgumentException")
    void compoundFilter_empty_throws() {
        FilterClause compound = FilterClause.newBuilder()
                .setCompoundFilter(CompoundFilter.newBuilder())
                .build();
        assertThatThrownBy(() -> buildFilter(compound))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one clause");
    }

    // =====================================================================
    // Exists Filter
    // =====================================================================

    @Test
    @DisplayName("Exists filter on sortable field produces FieldExistsQuery")
    void existsFilter_sortableField() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("quantity")
                .setExistsFilter(ExistsFilter.newBuilder().setMustExist(true))
                .build();
        Query q = buildFilter(fc);
        BooleanQuery bq = (BooleanQuery) q;
        Query inner = bq.clauses().get(0).query();
        assertThat(inner).isInstanceOf(FieldExistsQuery.class);
    }

    @Test
    @DisplayName("Exists must_not wraps with negation")
    void existsFilter_mustNotExist() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("quantity")
                .setExistsFilter(ExistsFilter.newBuilder().setMustExist(false))
                .build();
        Query q = buildFilter(fc);
        BooleanQuery bq = (BooleanQuery) q;
        Query inner = bq.clauses().get(0).query();
        // Negation wraps as BooleanQuery with MUST MatchAll + MUST_NOT exists
        assertThat(inner).isInstanceOf(BooleanQuery.class);
        BooleanQuery neg = (BooleanQuery) inner;
        boolean hasMustNot = neg.clauses().stream()
                .anyMatch(c -> c.occur() == BooleanClause.Occur.MUST_NOT);
        assertThat(hasMustNot).isTrue();
    }

    // =====================================================================
    // Empty Filter Clause
    // =====================================================================

    @Test
    @DisplayName("FilterClause with no predicate set throws")
    void emptyFilterClause_throws() {
        FilterClause fc = FilterClause.newBuilder()
                .setField("category")
                .build();
        assertThatThrownBy(() -> buildFilter(fc))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no filter predicate set");
    }

    @Test
    @DisplayName("Empty filter list returns null")
    void emptyFilterList_returnsNull() {
        Query q = FilterQueryBuilder.buildFilterQuery(List.of(), schema);
        assertThat(q).isNull();
    }

    // --- Helpers ---

    private Query buildFilter(FilterClause clause) {
        return FilterQueryBuilder.buildFilterQuery(List.of(clause), schema);
    }

    private static FilterClause termClause(String field, String value) {
        return FilterClause.newBuilder()
                .setField(field)
                .setTermFilter(TermFilter.newBuilder().setValue(value))
                .build();
    }

    private static FilterClause rangeClause(String field, String min, String max,
                                             boolean minExcl, boolean maxExcl) {
        return FilterClause.newBuilder()
                .setField(field)
                .setRangeFilter(RangeFilter.newBuilder()
                        .setMin(min).setMax(max)
                        .setMinExclusive(minExcl).setMaxExclusive(maxExcl))
                .build();
    }

    private IndexSchema buildSchema() {
        SchemaField id = new SchemaField();
        id.setName("id");
        id.setType(FieldDataType.STRING);
        id.setKey(true);
        id.setFilterable(true);

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);

        SchemaField content = new SchemaField();
        content.setName("content");
        content.setType(FieldDataType.STRING);
        content.setSearchable(true);
        // NOT filterable

        SchemaField quantity = new SchemaField();
        quantity.setName("quantity");
        quantity.setType(FieldDataType.INT32);
        quantity.setFilterable(true);
        quantity.setSortable(true);

        SchemaField price = new SchemaField();
        price.setName("price");
        price.setType(FieldDataType.DOUBLE);
        price.setFilterable(true);
        price.setSortable(true);

        SchemaField timestamp = new SchemaField();
        timestamp.setName("timestamp");
        timestamp.setType(FieldDataType.INT64);
        timestamp.setFilterable(true);

        SchemaField createdAt = new SchemaField();
        createdAt.setName("created_at");
        createdAt.setType(FieldDataType.DATE_TIME_OFFSET);
        createdAt.setFilterable(true);

        SchemaField location = new SchemaField();
        location.setName("location");
        location.setType(FieldDataType.GEO_POINT);
        location.setFilterable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, category, content, quantity, price,
                timestamp, createdAt, location});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}
