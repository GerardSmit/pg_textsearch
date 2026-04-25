CREATE EXTENSION IF NOT EXISTS pg_textsearch;

SET enable_seqscan = off;

DROP TABLE IF EXISTS highlight_docs CASCADE;
CREATE TABLE highlight_docs (
  id int PRIMARY KEY,
  title text,
  body text
);

INSERT INTO highlight_docs VALUES
  (1, 'Hello world', 'Hello brave world'),
  (2, 'No limits here', 'A limitation appears here'),
  (3, 'héllo world', NULL),
  (4, 'Hello hello', 'world only');

CREATE INDEX highlight_docs_idx ON highlight_docs USING bm25 (title, body)
  WITH (text_config = 'pg_catalog.simple');

-- Exact term, case-insensitive through text search normalization.
SELECT bm25_snippet(
  title,
  to_bm25query('hello', 'highlight_docs_idx'),
  field_name => 'title')
FROM highlight_docs WHERE id = 1;

SELECT bm25_snippet_positions(
  title,
  to_bm25query('hello', 'highlight_docs_idx'),
  field_name => 'title')
FROM highlight_docs WHERE id = 1;

-- Prefix query highlights the source token matched by dictionary expansion.
SELECT bm25_snippet(
  title,
  to_bm25query('hell*', 'highlight_docs_idx', grammar => true),
  field_name => 'title')
FROM highlight_docs WHERE id = 1;

-- Phrase spans are highlighted as one byte range.
SELECT bm25_snippet_positions(
  title,
  to_bm25query('title:"hello world"', 'highlight_docs_idx', grammar => true),
  field_name => 'title')
FROM highlight_docs WHERE id = 1;

SELECT bm25_snippet(
  title,
  to_bm25query('title:"hello world"', 'highlight_docs_idx', grammar => true),
  field_name => 'title')
FROM highlight_docs WHERE id = 1;

-- Phrase-prefix spans include the final prefix-matched token.
SELECT bm25_snippet(
  body,
  to_bm25query('body:"hello brav*"', 'highlight_docs_idx', grammar => true),
  field_name => 'body')
FROM highlight_docs WHERE id = 1;

-- limit/offset and custom tags.
SELECT bm25_snippet(
  title,
  to_bm25query('hello', 'highlight_docs_idx'),
  NULL,
  '[',
  ']',
  150,
  1,
  1,
  'title')
FROM highlight_docs WHERE id = 4;

-- UTF-8 byte offsets are half-open byte ranges.
SELECT bm25_snippet_positions(
  title,
  to_bm25query('héllo', 'highlight_docs_idx'),
  field_name => 'title')
FROM highlight_docs WHERE id = 3;

-- Field scoping: title-scoped query does not highlight the body field.
SELECT bm25_snippet(
  body,
  to_bm25query('title:hello', 'highlight_docs_idx', grammar => true),
  field_name => 'body')
FROM highlight_docs WHERE id = 1;

-- Text-query overload.
SELECT bm25_snippet(title, 'hello', 'highlight_docs_idx')
FROM highlight_docs WHERE id = 1;

-- JSON helper returns one object keyed by index field names.
SELECT bm25_highlights(
  to_bm25query('title:hello body:brav*', 'highlight_docs_idx', grammar => true),
  'highlight_docs_idx',
  VARIADIC ARRAY[title, body])
FROM highlight_docs WHERE id = 1;

-- NULL fields produce empty JSON entries.
SELECT bm25_highlights(
  to_bm25query('hello', 'highlight_docs_idx'),
  'highlight_docs_idx',
  VARIADIC ARRAY[title, body])
FROM highlight_docs WHERE id = 3;

DROP TABLE highlight_docs;
DROP EXTENSION pg_textsearch CASCADE;

-- =============================================================
-- Edge-case tests appended below
-- =============================================================

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS hl_edge CASCADE;
CREATE TABLE hl_edge (
  id int PRIMARY KEY,
  body text
);
INSERT INTO hl_edge VALUES
  (1, 'the quick brown fox jumps over the lazy dog'),
  (2, 'searching for search results in the search engine'),
  (3, '');

CREATE INDEX hl_edge_idx ON hl_edge USING bm25 (body)
  WITH (text_config = 'pg_catalog.simple');

-- 1. Empty string input.
SELECT bm25_snippet('', to_bm25query('test', 'hl_edge_idx'));

-- 2. No matches: highlight text that does not contain query terms.
SELECT bm25_snippet(
  'the quick brown fox jumps over the lazy dog',
  to_bm25query('elephant', 'hl_edge_idx'));

-- 3. Overlapping query terms: prefix 'search*' matches 'searching'
--    and exact 'search' both highlight the same region.
SELECT bm25_snippet(
  body,
  to_bm25query('search* search', 'hl_edge_idx', grammar => true))
FROM hl_edge WHERE id = 2;

SELECT bm25_snippet_positions(
  body,
  to_bm25query('search* search', 'hl_edge_idx', grammar => true))
FROM hl_edge WHERE id = 2;

-- Clean up.
DROP TABLE hl_edge;
DROP EXTENSION pg_textsearch CASCADE;

