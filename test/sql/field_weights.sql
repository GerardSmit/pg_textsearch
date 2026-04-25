-- Phase 6.1b: per-field BM25 weight multipliers.
-- Validates:
--   - field_weights reloption parses decimal lists.
--   - Score for a field-tagged term is multiplied by the corresponding
--     weight at index-scan time.
--   - Wrong entry count is rejected at CREATE INDEX.
--   - Uniform index (no field_weights) is unaffected (control).

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

SET enable_seqscan = off;

DROP TABLE IF EXISTS fw_docs CASCADE;
CREATE TABLE fw_docs (
  id    int PRIMARY KEY,
  title text,
  body  text
);
INSERT INTO fw_docs VALUES
  (1, 'postgres rules',        'rust is fast'),
  (2, 'database fundamentals', 'postgres is great'),
  (3, 'cats and dogs',         'kittens are cute'),
  (4, 'postgres and rust',     'postgres postgres postgres');

-- Control index: uniform weights (default).
CREATE INDEX fw_uniform ON fw_docs USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.english');

-- Title weighted 10x; body at default weight.
CREATE INDEX fw_title_heavy ON fw_docs USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.english', field_weights = '10.0,1.0');

-- Body weighted 10x.
CREATE INDEX fw_body_heavy ON fw_docs USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.english', field_weights = '1.0,10.0');

-- UNIFORM: doc 4 (postgres ×3 body + ×1 title) ranks first, then 1 and 2 tie.
SELECT id, round(bm25_get_current_score()::numeric, 3) AS score
FROM fw_docs
ORDER BY title <@> to_bm25query('postgres', 'fw_uniform')
LIMIT 5;

-- TITLE_HEAVY: title-only match (id 1) outranks body-only match (id 2)
-- by ~10x. doc 4 still wins because it has title AND body.
SELECT id, round(bm25_get_current_score()::numeric, 3) AS score
FROM fw_docs
ORDER BY title <@> to_bm25query('postgres', 'fw_title_heavy')
LIMIT 5;

-- BODY_HEAVY: body-heavy doc 4 dominates; doc 2 (body-only) beats
-- doc 1 (title-only) by ~10x.
SELECT id, round(bm25_get_current_score()::numeric, 3) AS score
FROM fw_docs
ORDER BY title <@> to_bm25query('postgres', 'fw_body_heavy')
LIMIT 5;

-- Field-qualified query routes the whole score through the title
-- weight.  In fw_title_heavy this is 10x the uniform score.
SELECT id, round(bm25_get_current_score()::numeric, 3) AS score
FROM fw_docs
ORDER BY title <@> to_bm25query('title:postgres', 'fw_title_heavy', grammar => true)
LIMIT 5;

SELECT id, round(bm25_get_current_score()::numeric, 3) AS score
FROM fw_docs
ORDER BY title <@> to_bm25query('title:postgres', 'fw_uniform', grammar => true)
LIMIT 5;

-- Wrong entry count is rejected.
DO $$
BEGIN
  EXECUTE 'CREATE INDEX fw_bad_count ON fw_docs USING bm25 (title, body)
            WITH (text_config = ''pg_catalog.english'',
                  field_weights = ''1.0'')';
  RAISE EXCEPTION 'expected error for wrong field_weights count';
EXCEPTION WHEN invalid_parameter_value THEN
  RAISE NOTICE 'wrong entry count rejected as expected';
END $$;

-- Non-numeric entry is rejected.
DO $$
BEGIN
  EXECUTE 'CREATE INDEX fw_bad_num ON fw_docs USING bm25 (title, body)
            WITH (text_config = ''pg_catalog.english'',
                  field_weights = ''abc,1.0'')';
  RAISE EXCEPTION 'expected error for non-numeric field_weights entry';
EXCEPTION WHEN invalid_parameter_value THEN
  RAISE NOTICE 'non-numeric entry rejected as expected';
END $$;

-- Zero / negative entry is rejected.
DO $$
BEGIN
  EXECUTE 'CREATE INDEX fw_bad_zero ON fw_docs USING bm25 (title, body)
            WITH (text_config = ''pg_catalog.english'',
                  field_weights = ''0,1.0'')';
  RAISE EXCEPTION 'expected error for zero field_weights entry';
EXCEPTION WHEN invalid_parameter_value THEN
  RAISE NOTICE 'zero entry rejected as expected';
END $$;

DROP TABLE fw_docs;
DROP EXTENSION pg_textsearch CASCADE;
