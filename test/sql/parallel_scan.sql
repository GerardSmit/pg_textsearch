-- Test case: parallel_scan
-- Verifies that parallel index scan produces identical results to serial scan.
--
-- Strategy:
--   1. Create a table with enough rows and spill multiple times to produce
--      3-4 on-disk segments.
--   2. Run queries serially (max_parallel_workers_per_gather = 0) and save
--      results to a temp table.
--   3. Run the same queries with parallel forced (debug_parallel_query = on,
--      max_parallel_workers_per_gather = 4, zero costs) and save results.
--   4. Compare via EXCEPT in both directions: any difference is a failure.
--   5. Test single-term queries (eligible for parallel scan) and multi-term
--      queries (should fall back to serial, still correct).

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

SET enable_seqscan = off;

--------------------------------------------------------------------------------
-- Setup: create table, index, and spill to produce multiple segments
--------------------------------------------------------------------------------

CREATE TABLE parallel_scan_test (
    id SERIAL PRIMARY KEY,
    content TEXT
);

CREATE INDEX parallel_scan_idx ON parallel_scan_test USING bm25(content)
    WITH (text_config='english');

-- Insert batch 1 and spill -> segment 1
INSERT INTO parallel_scan_test (content)
SELECT 'alpha bravo charlie document ' || i || ' extra filler words here'
FROM generate_series(1, 5000) AS i;

SELECT bm25_spill_index('parallel_scan_idx') IS NOT NULL AS spill_1;

-- Insert batch 2 and spill -> segment 2
INSERT INTO parallel_scan_test (content)
SELECT 'delta echo foxtrot document ' || i || ' more padding text added'
FROM generate_series(5001, 10000) AS i;

SELECT bm25_spill_index('parallel_scan_idx') IS NOT NULL AS spill_2;

-- Insert batch 3 and spill -> segment 3
INSERT INTO parallel_scan_test (content)
SELECT 'alpha delta golf document ' || i || ' yet another set of words'
FROM generate_series(10001, 15000) AS i;

SELECT bm25_spill_index('parallel_scan_idx') IS NOT NULL AS spill_3;

-- Insert batch 4 (stays in memtable, so we test memtable + segments)
INSERT INTO parallel_scan_test (content)
SELECT 'alpha hotel india document ' || i || ' final batch of content'
FROM generate_series(15001, 17000) AS i;

ANALYZE parallel_scan_test;

-- Confirm we have multiple segments
SELECT bm25_summarize_index('parallel_scan_idx') IS NOT NULL AS has_summary;

--------------------------------------------------------------------------------
-- Test 1: Single-term query (eligible for parallel index scan)
--------------------------------------------------------------------------------

-- Serial results
SET max_parallel_workers_per_gather = 0;

CREATE TEMP TABLE serial_single AS
SELECT id, ROUND((content <@> to_bm25query('alpha', 'parallel_scan_idx'))::numeric, 6) AS score
FROM parallel_scan_test
ORDER BY content <@> to_bm25query('alpha', 'parallel_scan_idx')
LIMIT 100;

-- Parallel results
SET debug_parallel_query = on;
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

CREATE TEMP TABLE parallel_single AS
SELECT id, ROUND((content <@> to_bm25query('alpha', 'parallel_scan_idx'))::numeric, 6) AS score
FROM parallel_scan_test
ORDER BY content <@> to_bm25query('alpha', 'parallel_scan_idx')
LIMIT 100;

-- Reset to defaults before comparing
RESET debug_parallel_query;
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

-- Compare: serial minus parallel should be empty
SELECT COUNT(*) AS serial_minus_parallel_single
FROM (
    SELECT id, score FROM serial_single
    EXCEPT
    SELECT id, score FROM parallel_single
) diff;

-- Compare: parallel minus serial should be empty
SELECT COUNT(*) AS parallel_minus_serial_single
FROM (
    SELECT id, score FROM parallel_single
    EXCEPT
    SELECT id, score FROM serial_single
) diff;

-- Row counts should match
SELECT
    (SELECT COUNT(*) FROM serial_single) AS serial_count,
    (SELECT COUNT(*) FROM parallel_single) AS parallel_count;

DROP TABLE serial_single;
DROP TABLE parallel_single;

--------------------------------------------------------------------------------
-- Test 2: Multi-term query (falls back to serial, still correct)
--------------------------------------------------------------------------------

-- Serial results
SET max_parallel_workers_per_gather = 0;

CREATE TEMP TABLE serial_multi AS
SELECT id, ROUND((content <@> to_bm25query('alpha delta', 'parallel_scan_idx'))::numeric, 6) AS score
FROM parallel_scan_test
ORDER BY content <@> to_bm25query('alpha delta', 'parallel_scan_idx')
LIMIT 100;

-- Parallel settings (multi-term should fall back to serial internally)
SET debug_parallel_query = on;
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

CREATE TEMP TABLE parallel_multi AS
SELECT id, ROUND((content <@> to_bm25query('alpha delta', 'parallel_scan_idx'))::numeric, 6) AS score
FROM parallel_scan_test
ORDER BY content <@> to_bm25query('alpha delta', 'parallel_scan_idx')
LIMIT 100;

-- Reset to defaults before comparing
RESET debug_parallel_query;
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

-- Compare: serial minus parallel should be empty
SELECT COUNT(*) AS serial_minus_parallel_multi
FROM (
    SELECT id, score FROM serial_multi
    EXCEPT
    SELECT id, score FROM parallel_multi
) diff;

-- Compare: parallel minus serial should be empty
SELECT COUNT(*) AS parallel_minus_serial_multi
FROM (
    SELECT id, score FROM parallel_multi
    EXCEPT
    SELECT id, score FROM serial_multi
) diff;

-- Row counts should match
SELECT
    (SELECT COUNT(*) FROM serial_multi) AS serial_count,
    (SELECT COUNT(*) FROM parallel_multi) AS parallel_count;

DROP TABLE serial_multi;
DROP TABLE parallel_multi;

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------

DROP TABLE parallel_scan_test CASCADE;
DROP EXTENSION pg_textsearch CASCADE;
