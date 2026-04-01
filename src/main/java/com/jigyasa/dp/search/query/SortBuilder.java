package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.SortClause;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.util.List;

/**
 * Builds Lucene Sort from gRPC SortClause list.
 */
public final class SortBuilder {

    private SortBuilder() {}

    public static Sort build(List<SortClause> sortClauses, InitializedIndexSchema schema) {
        if (sortClauses.isEmpty()) return null;

        SortField[] sortFields = new SortField[sortClauses.size()];
        for (int i = 0; i < sortClauses.size(); i++) {
            SortClause sc = sortClauses.get(i);
            SchemaField sf = schema.getFieldLookupMap().get(sc.getField());
            if (sf == null) throw new IllegalArgumentException("Unknown sort field: " + sc.getField());
            if (!sf.isSortable()) throw new IllegalArgumentException("Field is not sortable: " + sc.getField());

            String sortFieldName = LuceneFieldType.SORTABLE.toLuceneFieldName(sc.getField());

            if (sf.getType() == FieldDataType.GEO_POINT || sf.getType() == FieldDataType.GEO_POINT_COLLECTION) {
                if (!sc.hasGeoOrigin()) {
                    throw new IllegalArgumentException("GEO_POINT sort requires geo_origin (lat/lon center point)");
                }
                double lat = sc.getGeoOrigin().getLat();
                double lon = sc.getGeoOrigin().getLon();
                if (lat < -90.0 || lat > 90.0) {
                    throw new IllegalArgumentException("Latitude must be between -90 and 90, got: " + lat);
                }
                if (lon < -180.0 || lon > 180.0) {
                    throw new IllegalArgumentException("Longitude must be between -180 and 180, got: " + lon);
                }
                SortField geoSort = LatLonDocValuesField.newDistanceSort(sortFieldName, lat, lon);
                // Geo distance sort is always nearest-first; reverse not supported by Lucene
                if (sc.getDescending()) {
                    throw new IllegalArgumentException("Descending geo-distance sort is not supported; results are always nearest-first");
                }
                sortFields[i] = geoSort;
            } else {
                SortField.Type sortType = mapToSortFieldType(sf.getType());
                sortFields[i] = new SortField(sortFieldName, sortType, sc.getDescending());
            }
        }
        return new Sort(sortFields);
    }

    private static SortField.Type mapToSortFieldType(FieldDataType type) {
        return switch (type) {
            case INT32 -> SortField.Type.INT;
            case INT64, DATE_TIME_OFFSET -> SortField.Type.LONG;
            case DOUBLE -> SortField.Type.DOUBLE;
            case STRING, BOOLEAN -> SortField.Type.STRING;
            default -> throw new IllegalArgumentException("Sort not supported for type: " + type);
        };
    }
}
