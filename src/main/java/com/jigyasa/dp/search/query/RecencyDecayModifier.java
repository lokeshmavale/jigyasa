package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.utils.SystemFields;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;

/**
 * Applies exponential time-decay boost based on _indexed_at field.
 * Recent documents score higher. Configurable half-life.
 * Only active when: (a) recency_decay is set on request, (b) schema has ttlEnabled=true.
 */
public class RecencyDecayModifier implements QueryModifier {

    private static final int DEFAULT_HALF_LIFE_SECS = 3600; // 1 hour

    @Override
    public boolean applies(QueryContext context) {
        return context.request().hasRecencyDecay()
                && context.indexSchema().isTtlEnabled();
    }

    @Override
    public Query modify(Query query, QueryContext context) {
        int halfLifeSecs = context.request().getRecencyDecay().getHalfLifeSeconds();
        if (halfLifeSecs <= 0) halfLifeSecs = DEFAULT_HALF_LIFE_SECS;

        double halfLifeMs = halfLifeSecs * 1000.0;
        double decayConstant = Math.log(2) / halfLifeMs;
        long now = System.currentTimeMillis();

        // boost = exp(-λ * age), where age = now - _indexed_at
        DoubleValuesSource decaySource = DoubleValuesSource.fromField(
                SystemFields.INDEXED_AT,
                indexedAtMs -> {
                    if (indexedAtMs <= 0) return 1.0;
                    double ageMs = Math.max(0, now - indexedAtMs);
                    return Math.exp(-decayConstant * ageMs);
                }
        );

        return FunctionScoreQuery.boostByValue(query, decaySource);
    }
}
