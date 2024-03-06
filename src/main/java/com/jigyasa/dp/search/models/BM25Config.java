package com.jigyasa.dp.search.models;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class BM25Config {
    public static final float DEF_K1 = 1.2f;
    public static final float DEF_B = 0.75f;

    private Float k1;
    private Float b;

    public Float getB() {
        return Objects.requireNonNullElse(b, DEF_B);
    }

    public Float getK1() {
        return Objects.requireNonNullElse(k1, DEF_K1);
    }
}
