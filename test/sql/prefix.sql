-- Phase 1 regression: prefix queries via foo* grammar and bm25_prefix() sugar.
-- Validates parser, dictionary expansion (memtable + segments), expansion cap,
-- error paths, and standalone scoring.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS prefix_docs;
CREATE TABLE prefix_docs (id int PRIMARY KEY, body text);
INSERT INTO prefix_docs VALUES
  (1,  'the quick brown fox jumps over the lazy dog'),
  (2,  'quick test of quickness today'),
  (3,  'queer query about quokkas'),
  (4,  'no matching content here for this row'),
  (5,  'qualified quantum quasi quark'),
  (6,  'apple banana cherry'),
  (7,  'application of applied math'),
  (8,  'apt aptitude apricot'),
  (9,  'rerun running runner runs'),
  (10, 'random unrelated content');

CREATE INDEX prefix_docs_idx ON prefix_docs USING bm25 (body)
  WITH (text_config = 'pg_catalog.english');

-- Force a segment so prefix expansion exercises both memtable and segment dict.
SELECT bm25_spill_index('prefix_docs_idx');

-- Insert into memtable post-spill.
INSERT INTO prefix_docs VALUES
  (11, 'queue queueing queues'),
  (12, 'apparatus apparatuses');

-- Sanity: existing plain-term path unchanged.
SELECT id FROM prefix_docs
  WHERE id IN (1, 2)
  ORDER BY body <@> to_bm25query('quick', 'prefix_docs_idx'), id
  LIMIT 10;

-- Prefix matches everything starting with qu (memtable + segment).
-- Match-set: 1, 2, 3, 5, 11 (each contains a qu-prefixed token after stem).
SELECT id FROM prefix_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('qu*', 'prefix_docs_idx', grammar => true) AS s
        FROM prefix_docs
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- bm25_prefix sugar matches the same set as 'app*'.
SELECT id FROM prefix_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> bm25_prefix('app', 'prefix_docs_idx') AS s
        FROM prefix_docs
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Prefix that matches no terms returns no rows with negative score.
SELECT count(*)
  FROM (
    SELECT id, body <@> to_bm25query('xyzzy*', 'prefix_docs_idx', grammar => true) AS s
      FROM prefix_docs
      ORDER BY 2, id
      LIMIT 100
  ) t WHERE t.s < 0;

-- Empty prefix is rejected at scan time.
DO $$
DECLARE
  v int;
BEGIN
  SELECT count(*) INTO v FROM prefix_docs
    WHERE body <@> to_bm25query('*', 'prefix_docs_idx', grammar => true) < 0;
  RAISE EXCEPTION 'expected error for lone *, got count=%', v;
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'lone * rejected as expected';
END $$;

-- Embedded * is rejected.
DO $$
DECLARE
  v int;
BEGIN
  SELECT count(*) INTO v FROM prefix_docs
    WHERE body <@> to_bm25query('foo*bar', 'prefix_docs_idx', grammar => true) < 0;
  RAISE EXCEPTION 'expected error for embedded *, got count=%', v;
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'embedded * rejected as expected';
END $$;

-- Mixing prefix and plain term (implicit OR). 'fox' alone matches doc 1.
-- 'qu*' matches 1, 2, 3, 5, 11.
SELECT id FROM prefix_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('qu* fox', 'prefix_docs_idx', grammar => true) AS s
        FROM prefix_docs
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Standalone scoring with a prefix: docs that DO contain qu-prefixed
-- tokens get negative scores, others 0.
SELECT id,
       (body <@> to_bm25query('qu*', 'prefix_docs_idx', grammar => true)) < 0 AS hit
  FROM prefix_docs
  ORDER BY id;

-- Expansion cap: with a cap of 1, only one a-prefixed term feeds BMW.
-- Cannot rely on which term wins (varies with dict ordering), but the
-- match set must be a subset of the unconstrained match.
SELECT count(*) > 0 AS some_results,
       count(*) <= 5 AS bounded_results
  FROM (
    SELECT id, body <@> to_bm25query('a*', 'prefix_docs_idx', grammar => true) AS s
      FROM prefix_docs
      ORDER BY 2, id
      LIMIT 100
  ) t WHERE t.s < 0;

SET pg_textsearch.max_prefix_expansions = 1;
SELECT count(*) > 0 AS some_results,
       count(*) <= 3 AS bounded_results
  FROM (
    SELECT id, body <@> to_bm25query('a*', 'prefix_docs_idx', grammar => true) AS s
      FROM prefix_docs
      ORDER BY 2, id
      LIMIT 100
  ) t WHERE t.s < 0;
RESET pg_textsearch.max_prefix_expansions;

DROP TABLE prefix_docs;

-- ============================================================
-- Extreme fan-out: 200 unique terms sharing same prefix
-- Expansion must be capped and deterministic across spill
-- ============================================================
CREATE TABLE prefix_fanout(id serial, content text);

-- Generate 200 unique 'xpfxNNN' terms (single token, no underscore)
INSERT INTO prefix_fanout(content)
SELECT 'xpfx' || lpad(i::text, 3, '0') || ' common baseline'
FROM generate_series(1, 200) i;

CREATE INDEX prefix_fanout_idx ON prefix_fanout
  USING bm25(content) WITH (text_config='simple');

-- With default cap (50), prefix 'xpfx*' should match <= 50 terms
-- even though 200 exist in the dictionary
SET pg_textsearch.max_prefix_expansions = 50;

SELECT count(*) AS pre_spill_matches FROM (
  SELECT id FROM prefix_fanout
  ORDER BY content <@> to_bm25query('xpfx*', 'prefix_fanout_idx', grammar => true)
  LIMIT 200
) t;

-- Save pre-spill result set
SELECT id INTO TEMP pre_spill_ids FROM prefix_fanout
  ORDER BY content <@> to_bm25query('xpfx*', 'prefix_fanout_idx', grammar => true)
  LIMIT 200;

-- Spill to segment
SELECT bm25_spill_index('prefix_fanout_idx');

-- Post-spill: same query must return identical results
-- (deterministic lexicographic cap across sources)
SELECT id INTO TEMP post_spill_ids FROM prefix_fanout
  ORDER BY content <@> to_bm25query('xpfx*', 'prefix_fanout_idx', grammar => true)
  LIMIT 200;

SELECT count(*) AS diff_count FROM (
  (SELECT id FROM pre_spill_ids EXCEPT SELECT id FROM post_spill_ids)
  UNION ALL
  (SELECT id FROM post_spill_ids EXCEPT SELECT id FROM pre_spill_ids)
) d;

-- Verify cap is respected: even with 200 terms, results bounded
SELECT count(*) <= 50 AS cap_respected FROM post_spill_ids;

DROP TABLE pre_spill_ids;
DROP TABLE post_spill_ids;
DROP TABLE prefix_fanout CASCADE;
RESET pg_textsearch.max_prefix_expansions;

DROP EXTENSION pg_textsearch CASCADE;
