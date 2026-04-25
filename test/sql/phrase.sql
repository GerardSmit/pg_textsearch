-- Phase 3 regression: phrase queries via "quoted" grammar and bm25_phrase().
-- Validates parser, positional verifier (heap re-fetch), implicit OR
-- behavior with mixed term + phrase clauses, and standalone scoring.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS phrase_docs;
CREATE TABLE phrase_docs (id int PRIMARY KEY, body text);
INSERT INTO phrase_docs VALUES
  (1,  'the quick brown fox jumps over the lazy dog'),
  (2,  'a brown fox quickly jumped'),
  (3,  'the lazy dog sleeps'),
  (4,  'quick! brown is the color'),
  (5,  'never quoted but contains quick brown'),
  (6,  'no relevant content'),
  (7,  'red car blue car green car'),
  (8,  'a red and blue and green car'),
  (9,  'database system performance tuning'),
  (10, 'the system database is tuned for performance');

CREATE INDEX phrase_docs_idx ON phrase_docs USING bm25 (body)
  WITH (text_config = 'pg_catalog.english');

-- Plain term sanity: existing path unchanged.
SELECT id FROM phrase_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('quick', 'phrase_docs_idx') s
        FROM phrase_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Phrase: "quick brown" matches docs 1, 4, 5 (each contains adjacent
-- quick→brown after stemming). Doc 2 has both stems but in reverse
-- order (brown before quickly).
SELECT id FROM phrase_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick brown"', 'phrase_docs_idx', grammar => true) s
        FROM phrase_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Phrase: "brown fox" matches docs 1, 2.
SELECT id FROM phrase_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"brown fox"', 'phrase_docs_idx', grammar => true) s
        FROM phrase_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- bm25_phrase() builder
SELECT id FROM phrase_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> bm25_phrase(ARRAY['brown','fox'], 'phrase_docs_idx') s
        FROM phrase_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Phrase that matches no doc: "fox brown".
SELECT count(*) FROM (
  SELECT id, body <@> to_bm25query('"fox brown"', 'phrase_docs_idx', grammar => true) s
    FROM phrase_docs ORDER BY 2, id LIMIT 100
) t WHERE s < 0;

-- Three-token phrase: "system performance tuning" only matches doc 9.
SELECT id FROM phrase_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"system performance tuning"',
                                       'phrase_docs_idx', grammar => true) s
        FROM phrase_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Standalone scoring agrees with index path.
SELECT id, (body <@> to_bm25query('"brown fox"', 'phrase_docs_idx', grammar => true)) < 0 AS hit
  FROM phrase_docs
  ORDER BY id;

-- Empty phrase rejected.
DO $$
DECLARE
  v int;
BEGIN
  SELECT count(*) INTO v FROM phrase_docs
    WHERE body <@> to_bm25query('""', 'phrase_docs_idx', grammar => true) < 0;
  RAISE EXCEPTION 'expected error for empty phrase, got count=%', v;
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'empty phrase rejected as expected';
END $$;

-- Unterminated phrase rejected.
DO $$
DECLARE
  v int;
BEGIN
  SELECT count(*) INTO v FROM phrase_docs
    WHERE body <@> to_bm25query('"unterminated', 'phrase_docs_idx', grammar => true) < 0;
  RAISE EXCEPTION 'expected error for unterminated phrase, got count=%', v;
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'unterminated phrase rejected as expected';
END $$;

DROP TABLE phrase_docs;
DROP EXTENSION pg_textsearch CASCADE;
