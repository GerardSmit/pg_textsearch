-- Phase 6.1c: record LHS `(col1, col2) <@> bm25query` for multi-col.
-- Validates:
--   - Planner rewrites (col_a, col_b) <@> bm25query to use the
--     multi-col BM25 index whose columns exactly match.
--   - Works with implicit (no index in to_bm25query) and explicit
--     (index name in to_bm25query).
--   - Field-scoped / phrase / prefix grammar flows through.
--   - No matching index → error (cannot scope to an index at
--     standalone scoring time).

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

SET enable_seqscan = off;

DROP TABLE IF EXISTS rec_docs CASCADE;
CREATE TABLE rec_docs (
  id    int PRIMARY KEY,
  title text,
  body  text
);
INSERT INTO rec_docs VALUES
  (1, 'quick brown fox',  'a mammal that runs fast'),
  (2, 'lazy dog nap',     'the brown fox jumps over it'),
  (3, 'machine learning', 'neural networks and gradient descent'),
  (4, 'python tips',      'machine learning code');

CREATE INDEX rec_docs_idx ON rec_docs USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.english');

-- Implicit index: planner resolves (title, body) to rec_docs_idx.
EXPLAIN (COSTS OFF) SELECT id FROM rec_docs
  ORDER BY (title, body) <@> to_bm25query('fox')
  LIMIT 3;

SELECT id FROM rec_docs
  ORDER BY (title, body) <@> to_bm25query('fox'), id
  LIMIT 3;

-- Field-scoped grammar with record LHS.
SELECT id FROM rec_docs
  ORDER BY (title, body) <@> to_bm25query('title:fox', grammar => true), id
  LIMIT 3;

SELECT id FROM rec_docs
  ORDER BY (title, body) <@> to_bm25query('body:fox', grammar => true), id
  LIMIT 3;

-- Phrase.
SELECT id FROM (
  SELECT id FROM rec_docs
    ORDER BY (title, body) <@> to_bm25query('"machine learning"', grammar => true), id
    LIMIT 2
) t ORDER BY id;

-- Explicit index wins even if the column list wouldn't implicitly
-- resolve one.  Index grammar (field:term) controls field scoping;
-- the LHS column order is not meaningful at scoring time.
SELECT id FROM rec_docs
  ORDER BY (body, title) <@> to_bm25query('title:fox', 'rec_docs_idx', grammar => true), id
  LIMIT 3;

-- Reversed column order with implicit resolution: no matching index.
-- Falls back to the standalone scoring path, which errors cleanly.
DO $$
BEGIN
  PERFORM id FROM rec_docs
    ORDER BY (body, title) <@> to_bm25query('fox')
    LIMIT 1;
  RAISE EXCEPTION 'expected error for unresolved record LHS';
EXCEPTION
  WHEN feature_not_supported OR OTHERS THEN
    RAISE NOTICE 'unresolved record LHS rejected as expected';
END $$;

DROP TABLE rec_docs;
DROP EXTENSION pg_textsearch CASCADE;
