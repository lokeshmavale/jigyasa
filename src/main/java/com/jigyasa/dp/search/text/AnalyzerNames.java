package com.jigyasa.dp.search.text;

/**
 * Registry of all supported analyzer names.
 *
 * <p>Use these constants in schema JSON as {@code "indexAnalyzer"} or {@code "searchAnalyzer"}:
 * <pre>{@code
 * {"name": "title", "type": "STRING", "searchable": true, "indexAnalyzer": "lucene.fr", "searchAnalyzer": "lucene.fr"}
 * }</pre>
 *
 * <p>All Lucene language analyzers from {@code lucene-analysis-common} are supported.
 * CJK, Japanese (Kuromoji), and Korean (Nori) require their respective Lucene modules.
 */
public final class AnalyzerNames {

    // ── Generic analyzers ───────────────────────────────────────────────────
    public static final String STANDARD   = "standard";    // Unicode text segmentation + lowercase
    public static final String SIMPLE     = "simple";      // Letter tokenizer + lowercase
    public static final String KEYWORD    = "keyword";     // No tokenization — entire value as one token
    public static final String WHITESPACE = "whitespace";  // Split on whitespace only

    // ── Language-specific analyzers ─────────────────────────────────────────
    // All use language-appropriate stemming, stopwords, and normalization.
    public static final String LUCENE_ARABIC      = "lucene.ar";
    public static final String LUCENE_ARMENIAN    = "lucene.hy";
    public static final String LUCENE_BASQUE      = "lucene.eu";
    public static final String LUCENE_BENGALI     = "lucene.bn";
    public static final String LUCENE_BRAZILIAN   = "lucene.br";
    public static final String LUCENE_BULGARIAN   = "lucene.bg";
    public static final String LUCENE_CATALAN     = "lucene.ca";
    public static final String LUCENE_CJK         = "lucene.cjk";
    public static final String LUCENE_CZECH       = "lucene.cs";
    public static final String LUCENE_DANISH      = "lucene.da";
    public static final String LUCENE_DUTCH       = "lucene.nl";
    public static final String LUCENE_ENGLISH     = "lucene.en";
    public static final String LUCENE_ESTONIAN    = "lucene.et";
    public static final String LUCENE_FINNISH     = "lucene.fi";
    public static final String LUCENE_FRENCH      = "lucene.fr";
    public static final String LUCENE_GALICIAN    = "lucene.gl";
    public static final String LUCENE_GERMAN      = "lucene.de";
    public static final String LUCENE_GREEK       = "lucene.el";
    public static final String LUCENE_HINDI       = "lucene.hi";
    public static final String LUCENE_HUNGARIAN   = "lucene.hu";
    public static final String LUCENE_INDONESIAN  = "lucene.id";
    public static final String LUCENE_IRISH       = "lucene.ga";
    public static final String LUCENE_ITALIAN     = "lucene.it";
    public static final String LUCENE_LATVIAN     = "lucene.lv";
    public static final String LUCENE_LITHUANIAN  = "lucene.lt";
    public static final String LUCENE_NORWEGIAN   = "lucene.no";
    public static final String LUCENE_PERSIAN     = "lucene.fa";
    public static final String LUCENE_POLISH      = "lucene.pl";
    public static final String LUCENE_PORTUGUESE  = "lucene.pt";
    public static final String LUCENE_ROMANIAN    = "lucene.ro";
    public static final String LUCENE_RUSSIAN     = "lucene.ru";
    public static final String LUCENE_SERBIAN     = "lucene.sr";
    public static final String LUCENE_SORANI      = "lucene.ckb";
    public static final String LUCENE_SPANISH     = "lucene.es";
    public static final String LUCENE_SWEDISH     = "lucene.sv";
    public static final String LUCENE_THAI        = "lucene.th";
    public static final String LUCENE_TURKISH     = "lucene.tr";
    public static final String LUCENE_UKRAINIAN   = "lucene.uk";

    private AnalyzerNames() {}
}
