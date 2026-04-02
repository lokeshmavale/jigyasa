# Example 07 — Multi-Language Analyzers

Demonstrates per-field analyzer configuration for multilingual search.
Jigyasa ships **42 built-in analyzers** (4 generic + 38 language-specific) —
no plugins, no configuration files, just a schema property.

## What you'll learn

1. Setting `indexAnalyzer` / `searchAnalyzer` per field in the schema
2. How language-specific stemming improves recall (French, German, Hindi, Japanese/CJK)
3. Comparing `standard` vs language-tuned analyzers on the same data
4. Using the `keyword` analyzer for exact-match fields

## Schema snippet

```json
{
  "name": "title_fr",
  "type": "STRING",
  "searchable": true,
  "indexAnalyzer": "lucene.fr",
  "searchAnalyzer": "lucene.fr"
}
```

## Supported analyzers

| Generic | Language-specific (sample) |
|---------|--------------------------|
| `standard` | `lucene.en` `lucene.fr` `lucene.de` `lucene.es` `lucene.hi` `lucene.cjk` `lucene.ar` `lucene.ru` `lucene.ja` … |
| `simple` | `lucene.pt` `lucene.it` `lucene.nl` `lucene.pl` `lucene.tr` `lucene.th` `lucene.uk` … |
| `keyword` | 38 languages total — see `AnalyzerNames.java` |
| `whitespace` | |

## Run

```bash
# From repo root (server must be running: ./gradlew run)

# Java
./gradlew :examples:07-multi-language-analyzers:run

# Python
cd examples/07-multi-language-analyzers/python
pip install grpcio grpcio-tools googleapis-common-protos
python multi_language_analyzers.py
```

## Prerequisites

- Jigyasa server running on `localhost:50051`
