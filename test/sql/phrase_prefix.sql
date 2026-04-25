-- Phase 4 regression: phrase-prefix queries via "foo bar*" grammar
-- and bm25_phrase_prefix(). The final phrase token's trailing '*'
-- matches any indexed token starting with the prefix at the
-- adjacent position.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS pp_docs;
CREATE TABLE pp_docs (id int PRIMARY KEY, body text);
INSERT INTO pp_docs VALUES
  (1, 'run shoes for runners'),
  (2, 'run shall begin shortly'),
  (3, 'shoes run faster'),                  -- reverse order, no match
  (4, 'run! show your speed'),              -- "run" then "show"
  (5, 'run swiftly across the field'),
  (6, 'a runner runs run runs run'),        -- many runs but no following 'sh*'
  (7, 'system performance tuning helps'),
  (8, 'system perform tools available');

CREATE INDEX pp_docs_idx ON pp_docs USING bm25 (body)
  WITH (text_config = 'pg_catalog.english');

-- "run sh*" matches docs whose adjacent tokens are "run" then a
-- token starting with "sh". After english stemming:
--   doc 1: run shoe → match (sh)
--   doc 2: run shall → match (sh)
--   doc 3: shoe run → reverse, no match
--   doc 4: run show → match (sh)
SELECT id FROM pp_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"run sh*"', 'pp_docs_idx', grammar => true) s
        FROM pp_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- bm25_phrase_prefix builder.
SELECT id FROM pp_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> bm25_phrase_prefix(ARRAY['run','sh'],
                                              'pp_docs_idx') s
        FROM pp_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Three-token phrase-prefix: "system perfo*"
-- doc 7: system performance → match (perfo)
-- doc 8: system perform → match (perfo)
SELECT id FROM pp_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"system perfo*"',
                                        'pp_docs_idx', grammar => true) s
        FROM pp_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- No match: "run xyz*"
SELECT count(*) FROM (
  SELECT id, body <@> to_bm25query('"run xyz*"', 'pp_docs_idx', grammar => true) s
    FROM pp_docs ORDER BY 2, id LIMIT 100
) t WHERE s < 0;

-- Standalone scoring path: doc-by-doc verification.
SELECT id, (body <@> to_bm25query('"run sh*"', 'pp_docs_idx', grammar => true)) < 0 AS hit
  FROM pp_docs
  ORDER BY id;

-- Empty prefix in phrase-prefix rejected.
DO $$
DECLARE v int;
BEGIN
  SELECT count(*) INTO v FROM pp_docs
    WHERE body <@> to_bm25query('"foo *"', 'pp_docs_idx', grammar => true) < 0;
  RAISE EXCEPTION 'expected error for empty phrase prefix, got count=%', v;
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'empty phrase prefix rejected as expected';
END $$;

DROP TABLE pp_docs;
DROP EXTENSION pg_textsearch CASCADE;
