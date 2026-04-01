package com.jigyasa.dp.search.text;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public final class AnalyzerFactory {
    private static final Map<String, Supplier<Analyzer>> DEFAULT_ANALYZER_SUPPLIER = getDefaultAnalyzers();

    private AnalyzerFactory() {}

    private static Map<String, Supplier<Analyzer>> getDefaultAnalyzers() {
        Map<String, Supplier<Analyzer>> result = new HashMap<>();
        result.put(AnalyzerNames.KEYWORD, KeywordAnalyzer::new);
        result.put(AnalyzerNames.LUCENE_ARABIC, ArabicAnalyzer::new);
        result.put(AnalyzerNames.STANDARD, StandardAnalyzer::new);
        result.put(AnalyzerNames.LUCENE_HINDI, HindiAnalyzer::new);
        result.put(AnalyzerNames.WHITESPACE, WhitespaceAnalyzer::new);
        result.put(AnalyzerNames.LUCENE_ENGLISH, EnglishAnalyzer::new);
        return ImmutableMap.copyOf(result);
    }

    public static Analyzer getAnalyzer(@NonNull final String name) {
        if (!DEFAULT_ANALYZER_SUPPLIER.containsKey(name)) {
            throw new RuntimeException("Couldn't find analyzer: " + name);
        }
        return DEFAULT_ANALYZER_SUPPLIER.get(name).get();
    }

}
