package com.jigyasa.dp.search.models;

import com.google.gson.annotations.SerializedName;
import com.jigyasa.dp.search.models.mappers.BooleanFieldMapper;
import com.jigyasa.dp.search.models.mappers.CollectionFieldMapper;
import com.jigyasa.dp.search.models.mappers.DoubleFieldMapper;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.models.mappers.GeoPointFieldMapper;
import com.jigyasa.dp.search.models.mappers.Int32FieldMapper;
import com.jigyasa.dp.search.models.mappers.Int64FieldMapper;
import com.jigyasa.dp.search.models.mappers.StringFieldMapper;
import com.jigyasa.dp.search.models.mappers.VectorFieldMapper;

import java.util.function.Supplier;

public enum FieldDataType {
    @SerializedName("STRING")
    STRING(StringFieldMapper::new),
    @SerializedName("STRING_COLLECTION")
    STRING_COLLECTION(() -> new CollectionFieldMapper(StringFieldMapper::new)),
    @SerializedName("BOOLEAN")
    BOOLEAN(BooleanFieldMapper::new),
    @SerializedName("BOOLEAN_COLLECTION")
    BOOLEAN_COLLECTION(() -> new CollectionFieldMapper(BooleanFieldMapper::new)),
    @SerializedName("INT32")
    INT32(Int32FieldMapper::new),
    @SerializedName("INT32_COLLECTION")
    INT32_COLLECTION(() -> new CollectionFieldMapper(Int32FieldMapper::new)),
    @SerializedName("INT64")
    INT64(Int64FieldMapper::new),
    @SerializedName("INT64_COLLECTION")
    INT64_COLLECTION(() -> new CollectionFieldMapper(Int64FieldMapper::new)),
    @SerializedName("DOUBLE")
    DOUBLE(DoubleFieldMapper::new),
    @SerializedName("DOUBLE_COLLECTION")
    DOUBLE_COLLECTION(() -> new CollectionFieldMapper(DoubleFieldMapper::new)),
    @SerializedName("DATE_TIME_OFFSET")
    DATE_TIME_OFFSET(Int64FieldMapper::new),
    @SerializedName("DATE_TIME_OFFSET_COLLECTION")
    DATE_TIME_OFFSET_COLLECTION(() -> new CollectionFieldMapper(Int64FieldMapper::new)),
    @SerializedName("GEO_POINT")
    GEO_POINT(GeoPointFieldMapper::new),
    @SerializedName("GEO_POINT_COLLECTION")
    GEO_POINT_COLLECTION(() -> new CollectionFieldMapper(GeoPointFieldMapper::new)),
    @SerializedName("VECTOR")
    VECTOR(VectorFieldMapper::new);

    final Supplier<FieldMapperStrategy> strategySupplier;

    FieldDataType(Supplier<FieldMapperStrategy> strategySupplier) {
        this.strategySupplier = strategySupplier;
    }

    public Supplier<FieldMapperStrategy> getFieldMapper() {
        return strategySupplier;
    }

    public static boolean isCollection(FieldDataType type) {
        return type.name().endsWith("_COLLECTION");
    }
}
