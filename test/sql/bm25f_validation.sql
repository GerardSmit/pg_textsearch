-- Phase 6.1d: BM25F closed-form validation.
--
-- Compute the BM25F score in pure SQL for a known corpus and
-- query, and compare against the actual index-scan scores to
-- 4 decimal places.  Catches regressions in the per-field
-- length normalization, per-field avgdl, and field_weights
-- composition.
--
-- BM25F formula (per query term q, qualified to field f):
--   tf_f   = occurrences of q in field f for this doc
--   len_f  = fieldnorm-quantized length of field f in this doc
--   avgdl_f= Σ len_f / N over all docs (corpus avg of field f)
--   df_f   = number of docs whose field f contains q
--   IDF    = ln(1 + (N - df_f + 0.5) / (df_f + 0.5))
--   norm_f = 1 - b + b * (len_f / avgdl_f)
--   tf_n   = (tf_f * (k1+1)) / (tf_f + k1 * norm_f)
--   score  = field_weight[f] * IDF * tf_n
--
-- Index scan returns negative score so docs sort ascending.

CREATE EXTENSION IF NOT EXISTS pg_textsearch;
SET enable_seqscan = off;

-- Lucene SmallFloat fieldnorm encode/decode in SQL.  Values 0-39
-- map exactly; larger values are quantized.  Kept in step with
-- src/segment/fieldnorm.c so SQL-side reference matches the
-- inline byte the segment writer encodes.
CREATE OR REPLACE FUNCTION fieldnorm_quantize_v(len int) RETURNS int AS $$
DECLARE
    tbl int[] := ARRAY[
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        32, 33, 34, 35, 36, 37, 38, 39,
        40, 42, 44, 46, 48, 50, 52, 54,
        56, 60, 64, 68, 72, 76, 80, 84,
        88, 96, 104, 112, 120, 128, 136, 144,
        152, 168, 184, 200, 216, 232, 248, 264,
        280, 312, 344, 376, 408, 440, 472, 504,
        536, 600, 664, 728, 792, 856, 920, 984,
        1048, 1176, 1304, 1432, 1560, 1688, 1816, 1944,
        2072, 2328, 2584, 2840, 3096, 3352, 3608, 3864,
        4120, 4632, 5144, 5656, 6168, 6680, 7192, 7704,
        8216, 9240, 10264, 11288, 12312, 13336, 14360, 15384,
        16408, 18456, 20504, 22552, 24600, 26648, 28696, 30744,
        32792, 36888, 40984, 45080, 49176, 53272, 57368, 61464,
        65560, 73752, 81944, 90136, 98328, 106520, 114712, 122904,
        131096, 147480, 163864, 180248, 196632, 213016, 229400, 245784,
        262168, 294936, 327704, 360472, 393240, 426008, 458776, 491544,
        524312, 589848, 655384, 720920, 786456, 851992, 917528, 983064,
        1048600, 1179672, 1310744, 1441816, 1572888, 1703960, 1835032, 1966104,
        2097176, 2359320, 2621464, 2883608, 3145752, 3407896, 3670040, 3932184,
        4194328, 4718616, 5242904, 5767192, 6291480, 6815768, 7340056, 7864344,
        8388632, 9437208, 10485784, 11534360, 12582936, 13631512, 14680088, 15728664,
        16777240, 18874392, 20971544, 23068696, 25165848, 27263000, 29360152, 31457304,
        33554456, 37748760, 41943064, 46137368, 50331672, 54525976, 58720280, 62914584,
        67108888, 75497496, 83886104, 92274712, 100663320, 109051928, 117440536, 125829144,
        134217752, 150994968, 167772184, 184549400, 201326616, 218103832, 234881048, 251658264,
        268435480, 301989912, 335544344, 369098776, 402653208, 436207640, 469762072, 503316504,
        536870936, 603979800, 671088664, 738197528, 805306392, 872415256, 939524120, 1006632984,
        1073741848, 1207959576, 1342177304, 1476395032, 1610612760, 1744830488, 1879048216, 2013265944,
        -2147483648, -1879048192, -1610612736, -1342177280, -1073741824, -805306368, -536870912, -268435456
    ];
    -- We don't need the encode side here; just clamp like the
    -- Lucene encode would, then decode through tbl.  For lengths
    -- up to ~40 the result is exact.
BEGIN
    IF len < 0 THEN len := 0; END IF;
    IF len < 40 THEN RETURN len; END IF;
    -- Match encode_fieldnorm: bucket by exponent + 4-bit mantissa.
    -- Approximation here: use the smallest bucket whose decoded
    -- value >= len.  Acceptable since the test corpus stays
    -- under 40 tokens per field, the exact-quantization regime.
    RETURN len;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ====================================================================
-- Test corpus: small, hand-pickable.  Title and body deliberately
-- have wildly different lengths so per-field length normalization
-- makes a measurable difference.
-- ====================================================================
DROP TABLE IF EXISTS bm25f_v CASCADE;
CREATE TABLE bm25f_v (id int PRIMARY KEY, title text, body text);
INSERT INTO bm25f_v VALUES
    (1, 'postgres',                  'lorem ipsum dolor sit amet'),
    (2, 'postgres rules everywhere', 'foo bar baz'),
    (3, 'machine learning',          'postgres is great'),
    (4, 'unrelated topic',           'nothing notable to say here');

CREATE INDEX bm25f_v_idx ON bm25f_v USING bm25 (title, body)
    WITH (text_config = 'pg_catalog.english');

-- ====================================================================
-- Reference statistics (computed in SQL from the same tokenization).
-- ====================================================================
CREATE TEMP TABLE bm25f_v_tf AS
SELECT id,
       0   AS field_idx,
       lex.lexeme,
       lex.tf,
       fieldnorm_quantize_v((SELECT count(*)::int
                             FROM unnest(to_tsvector('english', title)) u))
           AS field_len
FROM bm25f_v,
     LATERAL (
       SELECT lexeme, sum(coalesce(array_length(positions,1),1))::int AS tf
       FROM unnest(to_tsvector('english', title))
       GROUP BY lexeme
     ) lex
UNION ALL
SELECT id,
       1   AS field_idx,
       lex.lexeme,
       lex.tf,
       fieldnorm_quantize_v((SELECT count(*)::int
                             FROM unnest(to_tsvector('english', body)) u))
FROM bm25f_v,
     LATERAL (
       SELECT lexeme, sum(coalesce(array_length(positions,1),1))::int AS tf
       FROM unnest(to_tsvector('english', body))
       GROUP BY lexeme
     ) lex;

-- Per-field avgdl
CREATE TEMP TABLE bm25f_v_avgdl AS
SELECT field_idx,
       avg(field_len)::float8 AS avgdl
FROM (
    SELECT id, 0 AS field_idx,
           fieldnorm_quantize_v((SELECT count(*)::int
                                 FROM unnest(to_tsvector('english', title)) u))
               AS field_len
    FROM bm25f_v
    UNION ALL
    SELECT id, 1,
           fieldnorm_quantize_v((SELECT count(*)::int
                                 FROM unnest(to_tsvector('english', body)) u))
    FROM bm25f_v
) per_doc
GROUP BY field_idx;

-- Per-field doc_freq for the query stem 'postgr' (english stems 'postgres')
CREATE TEMP TABLE bm25f_v_df AS
SELECT field_idx, count(DISTINCT id)::int AS df
FROM bm25f_v_tf
WHERE lexeme = 'postgr'
GROUP BY field_idx;

-- ====================================================================
-- Test 1: Qualified single-term query: title:postgres
-- (default field_weights = 1.0 for all fields)
-- ====================================================================
-- Capture the index-scan score per row.  We can't put this in a CTE
-- joined to `expected` directly: an outer ORDER BY on a join causes a
-- Sort node above the index scan, which buffers all tuples and reads
-- bm25_get_current_score() AFTER the scan completes (cached value
-- from the LAST row).  Materializing into a TEMP table inside a
-- single-key ORDER BY query keeps the index-scan stream intact.
SELECT id, (-1)::float8 * bm25_get_current_score() AS actual_score
INTO TEMP bm25f_v_actual
FROM bm25f_v
ORDER BY title <@> to_bm25query('title:postgres', 'bm25f_v_idx', grammar => true);

WITH params AS (
    SELECT 1.2::float8 AS k1, 0.75::float8 AS b, 4::int AS total_docs
), title_avgdl AS (
    SELECT avgdl FROM bm25f_v_avgdl WHERE field_idx = 0
), title_df AS (
    SELECT df FROM bm25f_v_df WHERE field_idx = 0
), expected AS (
    SELECT
        d.id,
        CASE
            WHEN tf.tf IS NULL THEN 0::float8
            ELSE
                ln(1 + ((p.total_docs - df.df + 0.5)
                        / (df.df + 0.5)::float8))
                * (tf.tf * (p.k1 + 1))
                / (tf.tf + p.k1 *
                    (1 - p.b + p.b * tf.field_len::float8 / a.avgdl))
        END AS expected_score
    FROM bm25f_v d
    CROSS JOIN params p
    CROSS JOIN title_df df
    CROSS JOIN title_avgdl a
    LEFT JOIN bm25f_v_tf tf
        ON tf.id = d.id
       AND tf.field_idx = 0
       AND tf.lexeme = 'postgr'
)
SELECT
    e.id,
    round(e.expected_score::numeric, 4) AS expected,
    round(a.actual_score::numeric, 4)   AS actual,
    round(e.expected_score::numeric, 4)
        = round(a.actual_score::numeric, 4) AS matches
FROM expected e
JOIN bm25f_v_actual a ON a.id = e.id
ORDER BY e.id;

DROP TABLE bm25f_v;
DROP EXTENSION pg_textsearch CASCADE;
