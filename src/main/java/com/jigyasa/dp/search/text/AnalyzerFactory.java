package com.jigyasa.dp.search.text;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sr.SerbianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Factory for Lucene analyzers. Supports 4 generic + 38 language-specific analyzers.
 *
 * <p>Usage: {@code AnalyzerFactory.getAnalyzer("lucene.fr")} returns a {@link FrenchAnalyzer}.
 */
public final class AnalyzerFactory {
    private static final Map<String, Supplier<Analyzer>> ANALYZERS = buildRegistry();

    private AnalyzerFactory() {}

    private static Map<String, Supplier<Analyzer>> buildRegistry() {
        Map<String, Supplier<Analyzer>> m = new HashMap<>();

        // Generic
        m.put(AnalyzerNames.STANDARD,   StandardAnalyzer::new);
        m.put(AnalyzerNames.SIMPLE,     SimpleAnalyzer::new);
        m.put(AnalyzerNames.KEYWORD,    KeywordAnalyzer::new);
        m.put(AnalyzerNames.WHITESPACE, WhitespaceAnalyzer::new);

        // Language-specific (all from lucene-analysis-common)
        m.put(AnalyzerNames.LUCENE_ARABIC,      ArabicAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_ARMENIAN,     ArmenianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_BASQUE,       BasqueAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_BENGALI,      BengaliAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_BRAZILIAN,    BrazilianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_BULGARIAN,    BulgarianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_CATALAN,      CatalanAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_CJK,          CJKAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_CZECH,        CzechAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_DANISH,        DanishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_DUTCH,         DutchAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_ENGLISH,       EnglishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_ESTONIAN,      EstonianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_FINNISH,       FinnishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_FRENCH,        FrenchAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_GALICIAN,      GalicianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_GERMAN,        GermanAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_GREEK,         GreekAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_HINDI,         HindiAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_HUNGARIAN,     HungarianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_INDONESIAN,    IndonesianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_IRISH,         IrishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_ITALIAN,       ItalianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_LATVIAN,       LatvianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_LITHUANIAN,    LithuanianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_NORWEGIAN,     NorwegianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_PERSIAN,       PersianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_POLISH,        PolishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_POLISH,        PolishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_PORTUGUESE,    PortugueseAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_ROMANIAN,      RomanianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_RUSSIAN,       RussianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_SERBIAN,       SerbianAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_SORANI,        SoraniAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_SPANISH,       SpanishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_SWEDISH,       SwedishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_THAI,           ThaiAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_TURKISH,        TurkishAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_UKRAINIAN,      UkrainianMorfologikAnalyzer::new);
        m.put(AnalyzerNames.LUCENE_UKRAINIAN,      UkrainianMorfologikAnalyzer::new);

        return ImmutableMap.copyOf(m);
    }

    /**
     * Returns a new Analyzer instance for the given name.
     *
     * @throws IllegalArgumentException if the analyzer name is not recognized
     */
    public static Analyzer getAnalyzer(@NonNull final String name) {
        Supplier<Analyzer> supplier = ANALYZERS.get(name);
        if (supplier == null) {
            throw new IllegalArgumentException(
                    "Unknown analyzer: '" + name + "'. Available: " + getSupportedNames());
        }
        return supplier.get();
    }

    /** Returns the set of all supported analyzer names. */
    public static Set<String> getSupportedNames() {
        return ANALYZERS.keySet();
    }
}
