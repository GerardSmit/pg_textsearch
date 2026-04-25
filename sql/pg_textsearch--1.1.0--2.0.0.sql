-- Upgrade from 1.1.0 to 2.0.0
--
-- 2.0.0: Clean-slate on-disk format. Pre-2.0 indexes must be dropped
-- and recreated (REINDEX is not sufficient).
--
-- New features:
--   * prefix queries 'foo*' and bm25_prefix()
--   * phrase / phrase-prefix queries '"foo bar"' / '"foo bar*"'
--   * multi-column indexes with field:term grammar
--   * field_weights reloption for per-field BM25 scoring
--   * content_format reloption for HTML/Markdown normalization
--   * record LHS operator '(a, b) <@> bm25query'
--   * unified to_bm25query(text, index, grammar, fuzzy_max_distance)
--   * bm25_snippet / bm25_snippet_positions / bm25_headline
--   * bm25_prewarm for buffer cache warming

-- Verify loaded library matches this SQL script version
DO $$
DECLARE
    lib_ver text;
BEGIN
    lib_ver := pg_catalog.current_setting('pg_textsearch.library_version', true);
    IF lib_ver IS NULL THEN
        RAISE EXCEPTION
            'pg_textsearch library not loaded. '
            'Add pg_textsearch to shared_preload_libraries and restart.';
    END IF;
    IF lib_ver OPERATOR(pg_catalog.<>) '2.0.0' THEN
        RAISE EXCEPTION
            'pg_textsearch library version mismatch: loaded=%, expected=%. '
            'Restart the server after installing the new binary.',
            lib_ver, '2.0.0';
    END IF;
END $$;

-- Replace to_bm25query: drop old overloads, create unified version.
DROP FUNCTION IF EXISTS @extschema@.to_bm25query(text);
DROP FUNCTION IF EXISTS @extschema@.to_bm25query(text, text);

CREATE FUNCTION @extschema@.to_bm25query(
    input_text text,
    index_name text DEFAULT NULL,
    grammar boolean DEFAULT false,
    fuzzy_max_distance int DEFAULT 0)
RETURNS @extschema@.bm25query
AS 'MODULE_PATHNAME', 'to_tpquery_unified'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Record LHS operator for multi-col indexes.
CREATE FUNCTION @extschema@.bm25_record_bm25query_score(record, @extschema@.bm25query)
RETURNS float8
AS 'MODULE_PATHNAME', 'bm25_record_bm25query_score'
LANGUAGE C STABLE STRICT PARALLEL SAFE COST 1000;

CREATE OPERATOR @extschema@.<@> (
    LEFTARG = record,
    RIGHTARG = @extschema@.bm25query,
    PROCEDURE = @extschema@.bm25_record_bm25query_score
);

-- Prefix builder helpers.
CREATE FUNCTION @extschema@.bm25_prefix(prefix_text text)
RETURNS @extschema@.bm25query
AS $fn$
    SELECT @extschema@.to_bm25query(prefix_text || '*', grammar => true)
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_prefix(prefix_text text, index_name text)
RETURNS @extschema@.bm25query
AS $fn$
    SELECT @extschema@.to_bm25query(prefix_text || '*', index_name, grammar => true)
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Phrase builder.
CREATE FUNCTION @extschema@.bm25_phrase(tokens text[])
RETURNS @extschema@.bm25query
AS $fn$
    SELECT CASE
        WHEN array_length(tokens, 1) IS NULL OR array_length(tokens, 1) = 0
            THEN @extschema@.to_bm25query('""', grammar => true)
        ELSE @extschema@.to_bm25query('"' || array_to_string(tokens, ' ') || '"', grammar => true)
    END
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_phrase(tokens text[], index_name text)
RETURNS @extschema@.bm25query
AS $fn$
    SELECT @extschema@.to_bm25query(
        '"' || array_to_string(tokens, ' ') || '"',
        index_name, grammar => true)
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Phrase-prefix builder.
CREATE FUNCTION @extschema@.bm25_phrase_prefix(tokens text[])
RETURNS @extschema@.bm25query
AS $fn$
    SELECT @extschema@.to_bm25query(
        '"' || array_to_string(tokens[1:array_length(tokens,1)-1], ' ') ||
        CASE WHEN array_length(tokens, 1) > 1 THEN ' ' ELSE '' END ||
        tokens[array_length(tokens, 1)] || '*"', grammar => true)
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_phrase_prefix(tokens text[], index_name text)
RETURNS @extschema@.bm25query
AS $fn$
    SELECT @extschema@.to_bm25query(
        '"' || array_to_string(tokens[1:array_length(tokens,1)-1], ' ') ||
        CASE WHEN array_length(tokens, 1) > 1 THEN ' ' ELSE '' END ||
        tokens[array_length(tokens, 1)] || '*"',
        index_name, grammar => true)
$fn$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Highlighting helpers.
CREATE FUNCTION @extschema@.bm25_snippet(
    document text,
    query @extschema@.bm25query,
    index_name text DEFAULT NULL,
    start_tag text DEFAULT '<b>',
    end_tag text DEFAULT '</b>',
    max_num_chars int DEFAULT 150,
    "limit" int DEFAULT NULL,
    "offset" int DEFAULT 0,
    field_name text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'bm25_snippet'
LANGUAGE C STABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_snippet(
    document text,
    query_text text,
    index_name text,
    start_tag text DEFAULT '<b>',
    end_tag text DEFAULT '</b>',
    max_num_chars int DEFAULT 150,
    "limit" int DEFAULT NULL,
    "offset" int DEFAULT 0,
    field_name text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'bm25_snippet_text'
LANGUAGE C STABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_snippet_positions(
    document text,
    query @extschema@.bm25query,
    index_name text DEFAULT NULL,
    "limit" int DEFAULT NULL,
    "offset" int DEFAULT 0,
    field_name text DEFAULT NULL)
RETURNS int4range[]
AS 'MODULE_PATHNAME', 'bm25_snippet_positions'
LANGUAGE C STABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_snippet_positions(
    document text,
    query_text text,
    index_name text,
    "limit" int DEFAULT NULL,
    "offset" int DEFAULT 0,
    field_name text DEFAULT NULL)
RETURNS int4range[]
AS 'MODULE_PATHNAME', 'bm25_snippet_positions_text'
LANGUAGE C STABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.bm25_headline(
    query @extschema@.bm25query,
    index_name text,
    VARIADIC fields text[])
RETURNS jsonb
AS 'MODULE_PATHNAME', 'bm25_headline'
LANGUAGE C STABLE PARALLEL SAFE;

-- Index prewarm helper.
CREATE FUNCTION @extschema@.bm25_prewarm(index_name text)
RETURNS int8
AS 'MODULE_PATHNAME', 'tp_prewarm_index'
LANGUAGE C VOLATILE STRICT;

DO $$
BEGIN
    RAISE INFO 'pg_textsearch upgraded to v2.0.0';
END
$$;
