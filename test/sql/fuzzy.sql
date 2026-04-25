-- Regression: fuzzy queries via to_bm25query(..., fuzzy_max_distance => N).
-- Validates typo-tolerant term and prefix expansion, memtable + segment
-- dictionaries, normal query isolation, and error paths.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS fuzzy_docs;
CREATE TABLE fuzzy_docs (id int PRIMARY KEY, body text);
INSERT INTO fuzzy_docs VALUES
  (1, 'hello world'),
  (2, 'help wanted'),
  (3, 'yellow bird'),
  (4, 'there are no limitations here'),
  (5, 'ordinary unrelated content');

CREATE INDEX fuzzy_docs_idx ON fuzzy_docs USING bm25 (body)
  WITH (text_config = 'pg_catalog.english');

-- Force a segment so fuzzy expansion exercises the on-disk dictionary.
SELECT bm25_spill_index('fuzzy_docs_idx');

-- Insert into memtable post-spill.
INSERT INTO fuzzy_docs VALUES
  (6, 'hullo from the memtable'),
  (7, 'limitless options');

-- hllo is one edit from both hello (segment) and hullo (memtable).
SELECT id FROM fuzzy_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('hllo', 'fuzzy_docs_idx', fuzzy_max_distance => 1) AS s
        FROM fuzzy_docs
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- A normal bm25query does not perform fuzzy expansion.
SELECT count(*)
  FROM (
    SELECT id, body <@> to_bm25query('hllo', 'fuzzy_docs_idx') AS s
      FROM fuzzy_docs
      ORDER BY 2, id
      LIMIT 100
  ) t WHERE t.s < 0;

-- Fuzzy-prefix compares the typed prefix to indexed term prefixes.
SELECT id FROM fuzzy_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('limt*', 'fuzzy_docs_idx', fuzzy_max_distance => 1, grammar => true) AS s
        FROM fuzzy_docs
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Optional index name form works in ORDER BY index scans.
SELECT id FROM fuzzy_docs
  ORDER BY body <@> to_bm25query('hllo', fuzzy_max_distance => 1), id
  LIMIT 2;

DO $$
DECLARE
  v int;
BEGIN
  SELECT count(*) INTO v FROM fuzzy_docs
    WHERE body <@> to_bm25query('hello', 'fuzzy_docs_idx', fuzzy_max_distance => 0) < 0;
  RAISE EXCEPTION 'expected error for distance 0, got count=%', v;
EXCEPTION WHEN invalid_parameter_value THEN
  RAISE NOTICE 'distance 0 rejected as expected';
END $$;

DROP TABLE fuzzy_docs;
DROP EXTENSION pg_textsearch CASCADE;

-- =============================================================
-- Edge-case tests appended below
-- =============================================================

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

-- 1. Fuzzy with field scope on a multi-column index.
DROP TABLE IF EXISTS fuzzy_mc CASCADE;
CREATE TABLE fuzzy_mc (
  id int PRIMARY KEY,
  title text,
  body text
);
INSERT INTO fuzzy_mc VALUES
  (1, 'hello world', 'unrelated content'),
  (2, 'unrelated title', 'hello there'),
  (3, 'goodbye world', 'farewell friends');

CREATE INDEX fuzzy_mc_idx ON fuzzy_mc USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.simple');

-- title:hllo should fuzzy-match "hello" only in the title field.
SELECT id FROM fuzzy_mc
  WHERE id IN (
    SELECT id FROM (
      SELECT id, title <@> to_bm25query('title:hllo', 'fuzzy_mc_idx', fuzzy_max_distance => 1, grammar => true) AS s
        FROM fuzzy_mc
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- 2. Fuzzy with a very long term (100+ chars) — should return no matches.
SELECT count(*)
  FROM (
    SELECT id, title <@> to_bm25query(
      'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij',
      'fuzzy_mc_idx', fuzzy_max_distance => 1) AS s
      FROM fuzzy_mc
      ORDER BY 2, id
      LIMIT 100
  ) t WHERE t.s < 0;

-- 3. Multiple fuzzy terms in one query.
DROP TABLE IF EXISTS fuzzy_multi CASCADE;
CREATE TABLE fuzzy_multi (
  id int PRIMARY KEY,
  body text
);
INSERT INTO fuzzy_multi VALUES
  (1, 'hello world'),
  (2, 'help wanted'),
  (3, 'hello wanted');

CREATE INDEX fuzzy_multi_idx ON fuzzy_multi USING bm25 (body)
  WITH (text_config = 'pg_catalog.simple');

-- "hllo wantd" fuzzy-matches both terms; doc 3 matches both.
SELECT id FROM fuzzy_multi
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('hllo wantd', 'fuzzy_multi_idx', fuzzy_max_distance => 1) AS s
        FROM fuzzy_multi
        ORDER BY 2, id
        LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Clean up.
DROP TABLE fuzzy_mc;
DROP TABLE fuzzy_multi;
DROP EXTENSION pg_textsearch CASCADE;

