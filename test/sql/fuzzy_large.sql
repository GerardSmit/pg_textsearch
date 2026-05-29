-- Regression test: fuzzy matching over a large corpus, spanning BOTH storage
-- layers (on-disk segment AND in-memory memtable), with index-scan vs
-- standalone (seq-scan) parity.
--
-- Rationale: the small fuzzy tests only exercise a handful of rows. This test
-- plants known 1- and 2-edit near-misses of a base term ("helicopter") among
-- 2000 unrelated filler rows. A bulk CREATE INDEX writes those straight to an
-- on-disk segment; a few post-build INSERTs then land in the in-memory
-- memtable. Fuzzy expansion must therefore find matches in:
--   * the segment dictionary (tp_segment_collect_fuzzy_terms),
--   * the memtable dictionary (tp_memtable_collect_fuzzy_terms), and
--   * BOTH at once in a single query (cross-layer),
-- and the standalone (forced seq-scan) scorer must agree with the index
-- top-k scan in every case.
--
-- Uses the 'simple' text config so lexemes are exact lowercased tokens and
-- edit distances are unambiguous (english stemming would mangle the planted
-- near-misses).
--
-- NOTE: deliberately uses WHERE ... <@> ... < 0 to drive standalone scoring
-- as an explicit test target; see CLAUDE.md standalone-scoring exception.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS fuzzy_large CASCADE;
CREATE TABLE fuzzy_large (
    id   serial PRIMARY KEY,
    body text
);

-- 2000 filler rows. None contain a token within edit distance 2 of
-- "helicopter".
INSERT INTO fuzzy_large (body)
SELECT 'filler alpha' || g || ' beta common document number ' || g
FROM generate_series(1, 2000) g;

-- Planted near-misses of "helicopter" (10 chars) at ids 2001..2006. These
-- are present at CREATE INDEX time, so they live in the on-disk segment.
--   2001 exact          (distance 0)
--   2002 helicpter      (delete 'o')              distance 1
--   2003 helicapter     (substitute o -> a)       distance 1
--   2004 hellicopter    (insert 'l')              distance 1
--   2005 helciopter     (transpose 'ic' -> 'ci')  distance 1 (Damerau)
--   2006 helikoptor     (c->k and e->o)           distance 2
INSERT INTO fuzzy_large (body) VALUES
    ('helicopter rescue mission'),
    ('helicpter landing zone'),
    ('helicapter fuel check'),
    ('hellicopter night flight'),
    ('helciopter tail rotor'),
    ('helikoptor crash report');

CREATE INDEX fl_idx ON fuzzy_large
    USING bm25 (body)
    WITH (text_config = 'pg_catalog.simple');

-- =======================================================================
-- Phase 1: corpus is entirely in the on-disk segment (bulk build spills).
-- =======================================================================

-- 1a. Standalone (forced seq scan), distance 1: exactly the 5 docs <= 1 edit.
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS seg_standalone_d1
FROM fuzzy_large
WHERE body <@> to_bm25query('helicopter', 'fl_idx',
                            fuzzy_max_distance => 1) < 0;
COMMIT;

-- 1b. Index top-k (forced index scan): the same 5 planted docs rank first.
BEGIN;
SET LOCAL enable_seqscan = off;
SELECT id FROM (
    SELECT id
    FROM fuzzy_large
    ORDER BY body <@> to_bm25query('helicopter', 'fl_idx',
                                   fuzzy_max_distance => 1)
    LIMIT 5
) t
ORDER BY id;
COMMIT;

-- 1c. distance 2 additionally matches the 2-edit doc (6 total).
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS seg_standalone_d2
FROM fuzzy_large
WHERE body <@> to_bm25query('helicopter', 'fl_idx',
                            fuzzy_max_distance => 2) < 0;
COMMIT;

-- =======================================================================
-- Phase 2: insert another near-miss AFTER build -> it lives in the memtable
-- while ids 2001..2006 remain in the segment. Fuzzy must span both layers.
--   2007 helicoptre     (transpose 'er' -> 're")  distance 1 (Damerau)
-- =======================================================================
INSERT INTO fuzzy_large (body) VALUES ('helicoptre extra payload');

-- 2a. Standalone distance 1 now spans segment (5) + memtable (1) = 6.
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS crosslayer_standalone_d1
FROM fuzzy_large
WHERE body <@> to_bm25query('helicopter', 'fl_idx',
                            fuzzy_max_distance => 1) < 0;
COMMIT;

-- 2b. Index top-k distance 1: the segment 5 plus the memtable doc (id 2007).
BEGIN;
SET LOCAL enable_seqscan = off;
SELECT id FROM (
    SELECT id
    FROM fuzzy_large
    ORDER BY body <@> to_bm25query('helicopter', 'fl_idx',
                                   fuzzy_max_distance => 1)
    LIMIT 6
) t
ORDER BY id;
COMMIT;

-- =======================================================================
-- Phase 3: spill the memtable into the segment, then re-verify.
-- =======================================================================
SELECT bm25_spill_index('fl_idx') IS NOT NULL AS did_spill;

-- 3a. Standalone distance 1 after spill: still 6 (5 originals + helicoptre).
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS postspill_standalone_d1
FROM fuzzy_large
WHERE body <@> to_bm25query('helicopter', 'fl_idx',
                            fuzzy_max_distance => 1) < 0;
COMMIT;

-- 3b. distance 2 after spill: 5 (d1 from segment) + helikoptor + helicoptre = 7.
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS postspill_standalone_d2
FROM fuzzy_large
WHERE body <@> to_bm25query('helicopter', 'fl_idx',
                            fuzzy_max_distance => 2) < 0;
COMMIT;

-- 3c. A term far from every dictionary token returns no scored rows
-- (no spurious fuzzy matches at scale, across both layers).
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) AS no_spurious_matches
FROM fuzzy_large
WHERE body <@> to_bm25query('xylophonist', 'fl_idx',
                            fuzzy_max_distance => 2) < 0;
COMMIT;

DROP TABLE fuzzy_large CASCADE;
DROP EXTENSION pg_textsearch CASCADE;
