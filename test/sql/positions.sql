-- V6 positions regression: verify that per-token positions are
-- captured in the memtable, persisted to V6 segments on spill, and
-- consumed by the phrase verifier via the index fast path (no heap
-- re-fetch needed).

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

DROP TABLE IF EXISTS pos_docs;
CREATE TABLE pos_docs (id int PRIMARY KEY, body text);
INSERT INTO pos_docs VALUES
  (1,  'the quick brown fox jumps'),
  (2,  'quick brown quick'),
  (3,  'brown fox leaps'),
  (4,  'unrelated content'),
  (5,  'brown quick reversed'),
  (6,  'quick brown fox again'),
  (7,  'database system performance tuning guide'),
  (8,  'the system database is tuned for performance'),
  (9,  'apt apricot apple application'),
  (10, 'apple pie');

CREATE INDEX pos_docs_idx ON pos_docs USING bm25 (body)
  WITH (text_config = 'pg_catalog.english');

-- ============================================================
-- Phase A: memtable-only (no spill). Positions come from DSA.
-- ============================================================
SELECT 'A1 phrase memtable' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick brown"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Phrase-prefix still falls back to heap re-fetch (works but slower)
SELECT 'A2 phrase-prefix memtable' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick bro*"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- ============================================================
-- Phase B: force a spill → positions now live in a V6 segment.
-- ============================================================
SELECT bm25_spill_index('pos_docs_idx') IS NOT NULL AS did_spill;

SELECT 'B1 phrase segment' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick brown"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

SELECT 'B2 phrase across tokens' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"system performance tuning"',
                                       'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

SELECT 'B3 phrase-prefix segment' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick bro*"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- Negative case: phrase absent
SELECT 'B4 phrase negative' AS case, count(*) FROM (
  SELECT id, body <@> to_bm25query('"brown quick"', 'pos_docs_idx', grammar => true) s
    FROM pos_docs ORDER BY 2, id LIMIT 100
) t WHERE s < 0;

-- ============================================================
-- Phase C: mix — add new docs to the memtable after the spill.
-- Phrase query hits BOTH memtable (new docs) and segment (old docs).
-- ============================================================
INSERT INTO pos_docs VALUES
  (11, 'quick brown fox after spill'),
  (12, 'unrelated new content'),
  (13, 'fresh insert quick brown');

SELECT 'C1 mixed memtable+segment' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick brown"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

-- ============================================================
-- Phase D: standalone scoring agrees (no heap/segment distinction;
-- verifies against the input text directly).
-- ============================================================
SELECT 'D1 standalone hit' AS case,
       id,
       (body <@> to_bm25query('"quick brown"', 'pos_docs_idx', grammar => true)) < 0 AS hit
  FROM pos_docs
  WHERE id BETWEEN 1 AND 6
  ORDER BY id;

-- ============================================================
-- Phase E: force a merge — verify positions propagate through
-- compaction. Phrase queries on merged segments must still work
-- without heap re-fetch (v1.4 "V6.9" path).
-- ============================================================
-- Force a second spill so we have two L0 segments to merge.
SELECT bm25_spill_index('pos_docs_idx') IS NOT NULL AS did_spill2;
-- Force-merge all segments into one.
SELECT bm25_force_merge('pos_docs_idx');

-- After merge, phrase queries must still return the correct set.
SELECT 'E1 phrase after merge' AS case, id FROM pos_docs
  WHERE id IN (
    SELECT id FROM (
      SELECT id, body <@> to_bm25query('"quick brown"', 'pos_docs_idx', grammar => true) s
        FROM pos_docs ORDER BY 2, id LIMIT 100
    ) t WHERE s < 0
  )
  ORDER BY id;

DROP TABLE pos_docs;
DROP EXTENSION pg_textsearch CASCADE;
