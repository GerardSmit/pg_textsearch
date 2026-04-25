-- Phase 6.1-core: multi-column index with field-scoped grammar.
-- Validates:
--   - CREATE INDEX ... USING bm25 (col1, col2) accepts multi-column.
--   - Unqualified term expands to OR across fields.
--   - field:term / field:"phrase" / field:prefix* scope correctly.
--   - Unknown field name errors helpfully.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

-- enable_seqscan = off so the tiny demo table routes through the
-- index scan instead of seq + bm25 scoring.
SET enable_seqscan = off;

DROP TABLE IF EXISTS mc_articles CASCADE;
CREATE TABLE mc_articles (
  id int PRIMARY KEY,
  title text,
  description text
);
INSERT INTO mc_articles VALUES
  (1, 'quick brown fox',     'a mammal that runs fast'),
  (2, 'lazy dog nap',        'the brown fox jumps over it'),
  (3, 'machine learning',    'neural networks and gradient descent'),
  (4, 'python tips',         'machine learning code'),
  (5, 'geology',             'brown rocks and sediment');

CREATE INDEX mc_articles_idx ON mc_articles USING bm25 (title, description)
  WITH (text_config = 'pg_catalog.english');

-- Unqualified term: OR across fields → docs containing "fox" in ANY
-- column (1, 2)
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('fox', 'mc_articles_idx'), id
  LIMIT 3;

-- title:fox → only doc 1
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:fox', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- description:fox → only doc 2
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('description:fox', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- title:"machine learning" phrase → only doc 3
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:"machine learning"', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- description:"machine learning" → only doc 4
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('description:"machine learning"', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Unqualified phrase: either field → docs 3, 4
SELECT id FROM (
  SELECT id FROM mc_articles
    ORDER BY title <@> to_bm25query('"machine learning"', 'mc_articles_idx', grammar => true), id
    LIMIT 2
) t ORDER BY id;

-- title:quic* prefix → doc 1
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:quic*', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- description:brown → docs 2, 5
SELECT id FROM (
  SELECT id FROM mc_articles
    ORDER BY title <@> to_bm25query('description:brown', 'mc_articles_idx', grammar => true), id
    LIMIT 2
) t ORDER BY id;

-- field:term binds only to the next clause; "mammal" remains
-- unqualified and can match description.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:nope mammal', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Repeating a field for independent title-scoped terms remains valid.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:quick title:brown', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- title:(...) scopes every child clause to title; "mammal" only appears
-- in description, so this returns no rows.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:(nope mammal)', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- description:(...) scopes every child clause to description.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('description:(nope mammal)', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Grouped term + prefix.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:(quick brown*)', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Grouped phrase + term.
SELECT id FROM (
  SELECT id FROM mc_articles
    ORDER BY title <@> to_bm25query('title:("machine learning" tips)', 'mc_articles_idx', grammar => true), id
    LIMIT 2
) t ORDER BY id;

-- Escaped colon is literal query text, not an unknown field prefix.
DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('body\:fox', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE NOTICE 'escaped colon treated as literal text';
END $$;

-- Escaped '*' is literal text, not a prefix marker.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:quic\*', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Escaped parentheses are literal text, not a field group.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:\(quick\)', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Escaped quote inside phrase.
SELECT id FROM mc_articles
  ORDER BY title <@> to_bm25query('title:"machine \"learning\""', 'mc_articles_idx', grammar => true), id
  LIMIT 3;

-- Invalid grouped syntax errors clearly.
DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('title:()', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for empty field group';
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'empty field group rejected as expected';
END $$;

DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('title:(quick', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for unterminated field group';
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'unterminated field group rejected as expected';
END $$;

DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('title:(quick description:fox)', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for nested field scope';
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'nested field scope rejected as expected';
END $$;

DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('title:quick\', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for trailing backslash';
EXCEPTION WHEN syntax_error THEN
  RAISE NOTICE 'trailing backslash rejected as expected';
END $$;

-- Unknown field errors clearly.
DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('body:fox', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for unknown field "body"';
EXCEPTION WHEN undefined_column THEN
  RAISE NOTICE 'unknown field rejected as expected';
END $$;

DO $$
BEGIN
  PERFORM id FROM mc_articles
    ORDER BY title <@> to_bm25query('body:(fox)', 'mc_articles_idx', grammar => true)
    LIMIT 1;
  RAISE EXCEPTION 'expected error for unknown field group "body"';
EXCEPTION WHEN undefined_column THEN
  RAISE NOTICE 'unknown field group rejected as expected';
END $$;

DROP TABLE mc_articles;
DROP EXTENSION pg_textsearch CASCADE;
