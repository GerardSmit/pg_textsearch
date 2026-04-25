/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * query.c - bm25query data type implementation
 */
#include <postgres.h>

#include <access/genam.h>
#include <access/htup_details.h>
#include <access/relation.h>
#include <access/table.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_am.h>
#include <catalog/pg_class.h>
#include <catalog/pg_index.h>
#include <catalog/pg_inherits.h>
#include <commands/defrem.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <nodes/pg_list.h>
#include <nodes/value.h>
#include <stdlib.h>
#include <tsearch/ts_type.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/regproc.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <varatt.h>

#include "access/am.h"
#include "constants.h"
#include "index/metapage.h"
#include "index/resolve.h"
#include "index/state.h"
#include "memtable/memtable.h"
#include "memtable/posting.h"
#include "planner/hooks.h"
#include "scoring/bm25.h"
#include "scoring/phrase.h"
#include "segment/fieldnorm.h"
#include "segment/io.h"
#include "segment/segment.h"
#include "types/array.h"
#include "types/markup.h"
#include "types/query.h"
#include "types/query_parser.h"
#include "types/vector.h"

/*
 * Phase 6.1d: per-field standalone BM25F context.
 *
 * The text-LHS scorer (`bm25_text_bm25query_score`) takes one text arg.
 * For multi-col indexes the existing flattening loses the per-column
 * boundary, so a tagged term `title:foo` got scored against the
 * concatenated tsvector and total doc length — directionally right
 * but not exact BM25F.
 *
 * The record-LHS scorer (`bm25_record_bm25query_score`) has access to
 * every column via the deformed tuple.  It tokenizes each text-like
 * column separately, stashes the resulting per-field tsvectors and
 * fieldnorm-quantized lengths in this context, then delegates to the
 * text scorer.  When the scorer's grammar path encounters a tagged
 * term, it pulls tf and doc length from this context's per-field
 * tsvector instead of the flattened-text tsvector.
 *
 * Backend-local static.  Saved/restored around the inner call so
 * recursive scoring (rare) doesn't clobber outer state.
 */
typedef struct TpBm25fDocCtx
{
	int num_fields;
	TSVector
			field_tsvectors[TP_MAX_FIELDS]; /* per-column; NULL slot = empty */
	int32	field_lengths[TP_MAX_FIELDS];	/* fieldnorm-quantized */
} TpBm25fDocCtx;

static TpBm25fDocCtx *tp_bm25f_doc_ctx = NULL;

/*
 * Cache for per-query IDF values to avoid repeated segment lookups.
 *
 * When the <@> operator is called per-row (e.g., ORDER BY text <@> query),
 * we need to calculate IDF for each query term. Without caching, this
 * requires opening all segments for EVERY row, which is catastrophically
 * slow with 1.86M rows and multiple segments.
 *
 * The cache is stored in fn_extra and persists for the duration of the
 * query. IDF values are computed once on first call and reused.
 */
#define MAX_CACHED_TERMS 64

typedef struct TermIdfEntry
{
	char   term[NAMEDATALEN]; /* null-terminated term string */
	uint32 doc_freq;		  /* unified doc frequency (memtable + segments) */
	float4 idf;				  /* cached IDF value */
} TermIdfEntry;

typedef struct QueryScoreCache
{
	Oid			 index_oid;			/* index this cache is for */
	BlockNumber	 first_segment;		/* segment chain head at cache time */
	int32		 total_docs;		/* total docs at cache time */
	float4		 avg_doc_len;		/* avg doc length at cache time */
	int			 num_terms;			/* number of cached terms */
	bool		 cache_full_warned; /* true after limit-exceeded warning */
	TermIdfEntry terms[MAX_CACHED_TERMS];
} QueryScoreCache;

/* Local helper functions */

/*
 * Look up cached IDF for a term. Returns -1.0 if not found in cache.
 */
static float4
lookup_cached_idf(QueryScoreCache *cache, const char *term, uint32 *doc_freq)
{
	int i;

	if (!cache)
		return -1.0f;

	for (i = 0; i < cache->num_terms; i++)
	{
		if (strcmp(cache->terms[i].term, term) == 0)
		{
			if (doc_freq)
				*doc_freq = cache->terms[i].doc_freq;
			return cache->terms[i].idf;
		}
	}
	return -1.0f;
}

/*
 * Add a term's IDF to the cache.
 * Warns once per query if the cache limit is exceeded.
 */
static void
cache_term_idf(
		QueryScoreCache *cache, const char *term, uint32 doc_freq, float4 idf)
{
	if (!cache)
		return;

	if (cache->num_terms >= MAX_CACHED_TERMS)
	{
		/* Warn once when limit is first exceeded */
		if (!cache->cache_full_warned)
		{
			ereport(WARNING,
					(errmsg("BM25 IDF cache limit exceeded (%d terms), "
							"additional terms will not be cached",
							MAX_CACHED_TERMS)));
			cache->cache_full_warned = true;
		}
		return;
	}

	strlcpy(cache->terms[cache->num_terms].term, term, NAMEDATALEN);
	cache->terms[cache->num_terms].doc_freq = doc_freq;
	cache->terms[cache->num_terms].idf		= idf;
	cache->num_terms++;
}

/*
 * Check if the cache is valid for the current index state.
 */
static bool
cache_is_valid(
		QueryScoreCache *cache,
		Oid				 index_oid,
		BlockNumber		 first_segment,
		int32			 total_docs)
{
	if (!cache)
		return false;
	if (cache->index_oid != index_oid)
		return false;
	if (cache->first_segment != first_segment)
		return false;
	if (cache->total_docs != total_docs)
		return false;
	return true;
}

PG_FUNCTION_INFO_V1(tpquery_in);
PG_FUNCTION_INFO_V1(tpquery_out);
PG_FUNCTION_INFO_V1(tpquery_recv);
PG_FUNCTION_INFO_V1(tpquery_send);
PG_FUNCTION_INFO_V1(to_tpquery_unified);
PG_FUNCTION_INFO_V1(to_tpquery_text);
PG_FUNCTION_INFO_V1(to_tpquery_text_index);
PG_FUNCTION_INFO_V1(bm25_text_bm25query_score);
PG_FUNCTION_INFO_V1(bm25_text_text_score);
PG_FUNCTION_INFO_V1(bm25_textarray_bm25query_score);
PG_FUNCTION_INFO_V1(bm25_textarray_text_score);
PG_FUNCTION_INFO_V1(bm25_record_bm25query_score);
PG_FUNCTION_INFO_V1(tpquery_eq);
PG_FUNCTION_INFO_V1(bm25_get_current_score);

/*
 * bm25_get_current_score - stub function for ORDER BY optimization
 *
 * Returns the cached BM25 score from the most recent index scan tuple.
 * The planner replaces resjunk ORDER BY expressions with calls to this
 * function, avoiding expensive re-computation of scores already computed
 * by the index scan.
 */
Datum
bm25_get_current_score(PG_FUNCTION_ARGS pg_attribute_unused())
{
	PG_RETURN_FLOAT8(tp_get_cached_score());
}

/*
 * tpquery input function
 * Formats:
 *   "query_text" - simple query without index (InvalidOid)
 *   "index_name:query_text" - query with index name (resolved to OID)
 * Note: If query_text contains a colon, use to_tpquery() instead
 */
Datum
tpquery_in(PG_FUNCTION_ARGS)
{
	char	*str = PG_GETARG_CSTRING(0);
	char	*colon;
	TpQuery *result;
	bool	 fuzzy				= false;
	uint8	 fuzzy_max_distance = 0;

	if (strncmp(str, "fuzzy(", 6) == 0)
	{
		char *endptr;
		long  dist;

		dist = strtol(str + 6, &endptr, 10);
		if (endptr == str + 6 || endptr[0] != ')' || endptr[1] != ':' ||
			dist < 1 || dist > UINT8_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid fuzzy bm25query input syntax")));
		fuzzy			   = true;
		fuzzy_max_distance = (uint8)dist;
		str				   = endptr + 2;
	}

	/* Check for index name prefix (format: "index_name:query") */
	colon = strchr(str, ':');
	if (colon && colon != str)
	{
		/* Found colon and it's not at the start - extract index name */
		int	  index_name_len = colon - str;
		char *index_name	 = palloc(index_name_len + 1);
		char *query_text	 = colon + 1; /* Skip past : */

		/* Copy the index name */
		memcpy(index_name, str, index_name_len);
		index_name[index_name_len] = '\0';

		/* Create query with index name (resolves to OID) */
		result = create_tpquery_from_name_options(
				query_text, index_name, false, fuzzy, fuzzy_max_distance);
		pfree(index_name);
	}
	else
	{
		/* No index name prefix - create without index */
		result = create_tpquery_options(
				str, InvalidOid, false, false, fuzzy, fuzzy_max_distance);
	}

	PG_RETURN_POINTER(result);
}

/*
 * tpquery output function
 * Converts OID back to index name for display
 */
Datum
tpquery_out(PG_FUNCTION_ARGS)
{
	TpQuery	  *tpquery = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	StringInfo str	   = makeStringInfo();

	if (tpquery_is_fuzzy(tpquery))
		appendStringInfo(
				str, "fuzzy(%u):", tpquery_fuzzy_max_distance(tpquery));

	if (OidIsValid(tpquery->index_oid))
	{
		/* Format with index name: "index_name:query_text" */
		char *index_name = get_rel_name(tpquery->index_oid);
		char *query_text = get_tpquery_text(tpquery);

		if (index_name)
			appendStringInfo(str, "%s:%s", index_name, query_text);
		else
			/* Index was dropped - show OID for debugging */
			appendStringInfo(
					str, "[oid=%u]:%s", tpquery->index_oid, query_text);
	}
	else
	{
		/* Format without index: just the query text */
		char *query_text = get_tpquery_text(tpquery);

		appendStringInfo(str, "%s", query_text);
	}

	PG_RETURN_CSTRING(str->data);
}

/*
 * tpquery receive function (binary input)
 * Binary format v1: version (1 byte) + index_oid (4 bytes) + query_text_len
 *                   (4 bytes) + query_text
 * Binary format v2: version (1 byte) + flags (1 byte) + index_oid (4 bytes) +
 *                   query_text_len (4 bytes) + query_text
 * Binary format v3: version (1 byte) + flags (1 byte) +
 *                   fuzzy_max_distance (1 byte) + index_oid (4 bytes) +
 *                   query_text_len (4 bytes) + query_text
 */
Datum
tpquery_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
	TpQuery	  *result;
	uint8	   version;
	uint8	   flags = 0;
	Oid		   index_oid;
	int32	   query_text_len;
	char	  *query_text;
	bool	   explicit_index;
	bool	   fuzzy;
	uint8	   fuzzy_max_distance = 0;

	/* Read and validate version */
	version = pq_getmsgbyte(buf);
	if (version != 1 && version != 2 && version != 3)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unsupported bm25query binary format version %u",
						version),
				 errhint("Expected version 1, 2, or 3. This may indicate data "
						 "from "
						 "an incompatible pg_textsearch version.")));

	/* Read flags for v2+ */
	if (version >= 2)
		flags = pq_getmsgbyte(buf);
	if (version >= 3)
		fuzzy_max_distance = pq_getmsgbyte(buf);

	index_oid	   = pq_getmsgint(buf, sizeof(Oid));
	query_text_len = pq_getmsgint(buf, sizeof(int32));

	/* Validate length to prevent unbounded memory allocation */
	if (query_text_len < 0 || query_text_len > 1000000) /* 1MB limit */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid query text length: %d", query_text_len)));

	query_text = palloc(query_text_len + 1);
	pq_copymsgbytes(buf, query_text, query_text_len);
	query_text[query_text_len] = '\0';

	explicit_index = (flags & TPQUERY_FLAG_EXPLICIT_INDEX) != 0;
	fuzzy		   = (flags & TPQUERY_FLAG_FUZZY) != 0;
	{
		bool grammar = (flags & TPQUERY_FLAG_GRAMMAR) != 0;
		result		 = create_tpquery_options(
				  query_text,
				  index_oid,
				  explicit_index,
				  grammar,
				  fuzzy,
				  fuzzy_max_distance);
	}
	pfree(query_text);

	PG_RETURN_POINTER(result);
}

/*
 * tpquery send function (binary output)
 * Binary format v3: version (1 byte) + flags (1 byte) +
 *                   fuzzy_max_distance (1 byte) + index_oid (4 bytes) +
 *                   query_text_len (4 bytes) + query_text
 */
Datum
tpquery_send(PG_FUNCTION_ARGS)
{
	TpQuery		  *tpquery = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	StringInfoData buf;
	char		  *query_text;

	pq_begintypsend(&buf);
	pq_sendint8(&buf, TPQUERY_VERSION);
	pq_sendint8(&buf, tpquery->flags);
	pq_sendint8(&buf, tpquery->fuzzy_max_distance);
	pq_sendint32(&buf, tpquery->index_oid);
	pq_sendint32(&buf, tpquery->query_text_len);

	query_text = get_tpquery_text(tpquery);
	pq_sendbytes(&buf, query_text, tpquery->query_text_len);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Unified to_bm25query(text, text, bool, int) entry point.
 * Args: query_text, index_name (NULL ok), grammar, fuzzy_max_distance
 */
Datum
to_tpquery_unified(PG_FUNCTION_ARGS)
{
	text	*input_text;
	char	*query_text;
	char	*index_name = NULL;
	bool	 grammar	= false;
	int32	 fuzzy_dist = 0;
	TpQuery *result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	input_text = PG_GETARG_TEXT_PP(0);
	query_text = text_to_cstring(input_text);

	if (!PG_ARGISNULL(1))
		index_name = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (!PG_ARGISNULL(2))
		grammar = PG_GETARG_BOOL(2);

	if (!PG_ARGISNULL(3))
		fuzzy_dist = PG_GETARG_INT32(3);

	if (fuzzy_dist < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("fuzzy_max_distance must be >= 0")));
	if (fuzzy_dist > tp_max_fuzzy_distance)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("fuzzy_max_distance %d exceeds "
						"pg_textsearch.max_fuzzy_distance (%d)",
						fuzzy_dist,
						tp_max_fuzzy_distance)));

	result = create_tpquery_from_name_options(
			query_text,
			index_name,
			grammar,
			fuzzy_dist > 0,
			(uint8)fuzzy_dist);

	pfree(query_text);
	if (index_name)
		pfree(index_name);

	PG_RETURN_POINTER(result);
}

/* Backward-compat shims for old SQL versions (≤ 1.1.0). */
Datum
to_tpquery_text(PG_FUNCTION_ARGS)
{
	return to_tpquery_unified(fcinfo);
}

Datum
to_tpquery_text_index(PG_FUNCTION_ARGS)
{
	return to_tpquery_unified(fcinfo);
}

/*
 * Find the first child index of a partitioned index via pg_inherits.
 * Returns InvalidOid if no children found.
 */
static Oid
find_first_child_index(Oid parent_index_oid)
{
	Relation	inhrel;
	SysScanDesc scan;
	HeapTuple	tuple;
	ScanKeyData skey;
	Oid			child_oid = InvalidOid;

	inhrel = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(
			&skey,
			Anum_pg_inherits_inhparent,
			BTEqualStrategyNumber,
			F_OIDEQ,
			ObjectIdGetDatum(parent_index_oid));

	scan = systable_beginscan(
			inhrel, InheritsParentIndexId, true, NULL, 1, &skey);

	tuple = systable_getnext(scan);
	if (tuple != NULL)
	{
		Form_pg_inherits inhform = (Form_pg_inherits)GETSTRUCT(tuple);
		child_oid				 = inhform->inhrelid;
	}

	systable_endscan(scan);
	table_close(inhrel, AccessShareLock);

	return child_oid;
}

/*
 * Find the first child BM25 index for an inheritance parent index.
 * This handles hypertables where child indexes don't have pg_inherits
 * relationship to the parent index - we find via the table hierarchy.
 *
 * Returns InvalidOid if no matching child index found.
 */
static Oid
find_first_child_bm25_index(Oid parent_index_oid, const char *indexed_colname)
{
	Relation	  inhrel;
	SysScanDesc	  scan;
	HeapTuple	  tuple;
	ScanKeyData	  skey;
	HeapTuple	  idx_tuple;
	Form_pg_index idx_form;
	Oid			  parent_table_oid;
	Oid			  bm25_am_oid;
	Oid			  result = InvalidOid;

	if (!indexed_colname)
		return InvalidOid;

	/* Look up BM25 access method OID */
	bm25_am_oid = get_am_oid("bm25", true);
	if (!OidIsValid(bm25_am_oid))
		return InvalidOid;

	/* Get the table this index is on */
	idx_tuple =
			SearchSysCache1(INDEXRELID, ObjectIdGetDatum(parent_index_oid));
	if (!HeapTupleIsValid(idx_tuple))
		return InvalidOid;
	idx_form		 = (Form_pg_index)GETSTRUCT(idx_tuple);
	parent_table_oid = idx_form->indrelid;
	ReleaseSysCache(idx_tuple);

	/* Find first child table */
	inhrel = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(
			&skey,
			Anum_pg_inherits_inhparent,
			BTEqualStrategyNumber,
			F_OIDEQ,
			ObjectIdGetDatum(parent_table_oid));

	scan = systable_beginscan(
			inhrel, InheritsParentIndexId, true, NULL, 1, &skey);

	while ((tuple = systable_getnext(scan)) != NULL && result == InvalidOid)
	{
		Form_pg_inherits inhform		 = (Form_pg_inherits)GETSTRUCT(tuple);
		Oid				 child_table_oid = inhform->inhrelid;
		Relation		 child_table;
		List			*child_indexes;
		ListCell		*lc;

		child_table	  = table_open(child_table_oid, AccessShareLock);
		child_indexes = RelationGetIndexList(child_table);

		foreach (lc, child_indexes)
		{
			Oid			  child_idx_oid = lfirst_oid(lc);
			HeapTuple	  child_idx_tuple;
			HeapTuple	  child_class_tuple;
			Form_pg_index child_idx_form;
			Form_pg_class child_class_form;
			AttrNumber	  child_attnum;
			char		 *child_colname;

			child_idx_tuple = SearchSysCache1(
					INDEXRELID, ObjectIdGetDatum(child_idx_oid));
			if (!HeapTupleIsValid(child_idx_tuple))
				continue;

			child_idx_form = (Form_pg_index)GETSTRUCT(child_idx_tuple);

			/*
			 * Match by column name instead of raw attnum.
			 * Dropped columns can cause attnum drift between
			 * parent and child tables.
			 */
			if (child_idx_form->indnatts < 1)
			{
				ReleaseSysCache(child_idx_tuple);
				continue;
			}

			child_attnum  = child_idx_form->indkey.values[0];
			child_colname = get_attname(child_table_oid, child_attnum, true);

			if (!child_colname || strcmp(child_colname, indexed_colname) != 0)
			{
				ReleaseSysCache(child_idx_tuple);
				continue;
			}

			/* Check it's a BM25 index */
			child_class_tuple =
					SearchSysCache1(RELOID, ObjectIdGetDatum(child_idx_oid));
			if (!HeapTupleIsValid(child_class_tuple))
			{
				ReleaseSysCache(child_idx_tuple);
				continue;
			}

			child_class_form = (Form_pg_class)GETSTRUCT(child_class_tuple);

			if (child_class_form->relam == bm25_am_oid)
			{
				result = child_idx_oid;
				ReleaseSysCache(child_class_tuple);
				ReleaseSysCache(child_idx_tuple);
				break;
			}

			ReleaseSysCache(child_class_tuple);
			ReleaseSysCache(child_idx_tuple);
		}

		list_free(child_indexes);
		table_close(child_table, AccessShareLock);
	}

	systable_endscan(scan);
	table_close(inhrel, AccessShareLock);

	return result;
}

/*
 * Helper: Validate query and get index
 * Returns opened index relation, sets index_oid_out if provided
 *
 * For partitioned indexes (created on partitioned tables), this function
 * finds the first child index to use for text config. The actual corpus
 * stats are aggregated from all children later.
 */
static Relation
validate_and_open_index(TpQuery *query, Oid *index_oid_out)
{
	Oid		 index_oid = get_tpquery_index_oid(query);
	Relation index_rel;
	char	 relkind;

	if (!OidIsValid(index_oid))
	{
		/* No index resolved - return ERROR */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("text <@> tpquery operator requires index"),
				 errhint("Use to_tpquery(text, index_name) for standalone "
						 "scoring")));
	}

	/*
	 * Check if this is a partitioned index. Partitioned indexes don't have
	 * storage - they're just templates. We need to use a child index for
	 * text config, but aggregate stats from all children.
	 */
	relkind = get_rel_relkind(index_oid);
	if (relkind == RELKIND_PARTITIONED_INDEX)
	{
		Oid child_index_oid = find_first_child_index(index_oid);
		if (!OidIsValid(child_index_oid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("partitioned index has no child partitions"),
					 errdetail(
							 "The index \"%s\" is a partitioned index with "
							 "no partition indexes.",
							 get_rel_name(index_oid))));
		}
		/* Use the child index for text config access */
		index_rel = index_open(child_index_oid, AccessShareLock);
	}
	else
	{
		/* Open the index relation directly */
		index_rel = index_open(index_oid, AccessShareLock);
	}

	if (index_oid_out)
		*index_oid_out = index_oid;

	return index_rel;
}

/*
 * Helper: Calculate document length from tsvector
 */
static float4
calculate_doc_length(TSVector tsvector)
{
	WordEntry *entries		  = ARRPTR(tsvector);
	int		   doc_term_count = tsvector->size;
	float4	   doc_length	  = 0.0f;
	int		   i;

	for (i = 0; i < doc_term_count; i++)
	{
		int term_freq;
		if (entries[i].haspos)
			term_freq = (int32)POSDATALEN(tsvector, &entries[i]);
		else
			term_freq = 1;
		doc_length += term_freq;
	}

	return doc_length;
}

/*
 * Helper: Find term frequency in document
 */
static float4
find_term_frequency(
		TSVector tsvector, WordEntry *query_entry, char *query_lexeme)
{
	WordEntry *entries		  = ARRPTR(tsvector);
	char	  *lexemes_start  = STRPTR(tsvector);
	int		   doc_term_count = tsvector->size;
	int		   i;

	for (i = 0; i < doc_term_count; i++)
	{
		char *doc_lexeme = lexemes_start + entries[i].pos;
		if (entries[i].len == query_entry->len &&
			memcmp(doc_lexeme, query_lexeme, entries[i].len) == 0)
		{
			if (entries[i].haspos)
				return (int32)POSDATALEN(tsvector, &entries[i]);
			else
				return 1;
		}
	}

	return 0.0f; /* Term not found */
}

/*
 * Term-frequency lookup keyed by a raw cstring + length (for use
 * with resolved-term lists where we don't have a WordEntry handy).
 */
static float4
find_term_frequency_by_text(TSVector tsvector, const char *term, int term_len)
{
	WordEntry *entries		  = ARRPTR(tsvector);
	char	  *lexemes_start  = STRPTR(tsvector);
	int		   doc_term_count = tsvector->size;
	int		   i;

	for (i = 0; i < doc_term_count; i++)
	{
		char *doc_lexeme = lexemes_start + entries[i].pos;
		if (entries[i].len == term_len &&
			memcmp(doc_lexeme, term, term_len) == 0)
		{
			if (entries[i].haspos)
				return (int32)POSDATALEN(tsvector, &entries[i]);
			else
				return 1;
		}
	}
	return 0.0f;
}

/*
 * Helper: Calculate BM25 score for a single term
 */
static float4
calculate_term_score(
		float4 tf,
		float4 idf,
		float4 doc_length,
		float4 avg_doc_len,
		float4 k1,
		float4 b,
		int	   query_freq)
{
	double numerator_d;
	double denominator_d;
	float4 term_score;

	/* BM25 formula: IDF * tf*(k1+1) / (tf + k1*(1-b+b*dl/avgdl)) */
	numerator_d = (double)tf * ((double)k1 + 1.0);

	if (avg_doc_len > 0.0f)
	{
		denominator_d = (double)tf +
						(double)k1 * (1.0 - (double)b +
									  (double)b * ((double)doc_length /
												   (double)avg_doc_len));
	}
	else
	{
		denominator_d = (double)tf + (double)k1;
	}

	term_score = (float4)((double)idf * (numerator_d / denominator_d) *
						  (double)query_freq);

	return term_score;
}

/*
 * BM25 scoring function for text <@> bm25query operations
 *
 * This operator is called per-row when scoring documents, so we use fn_extra
 * to cache IDF values across rows. Without caching, each row would require
 * opening all segments for each query term - catastrophic for large indexes.
 */
Datum
bm25_text_bm25query_score(PG_FUNCTION_ARGS)
{
	text	*text_arg	= PG_GETARG_TEXT_PP(0);
	TpQuery *query		= (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
	char	*query_text = get_tpquery_text(query);
	Oid		 index_oid;

	Relation		   index_rel = NULL;
	TpIndexMetaPage	   metap	 = NULL;
	Oid				   text_config_oid;
	Datum			   tsvector_datum;
	TSVector		   tsvector;
	Datum			   query_tsvector_datum;
	TSVector		   query_tsvector;
	WordEntry		  *query_entries;
	char			  *query_lexemes_start;
	TpLocalIndexState *index_state;
	float4			   avg_doc_len;
	int32			   total_docs;
	int64			   total_len;
	float8			   result = 0.0;
	int				   q_i;
	float4			   doc_length;
	int				   query_term_count;
	QueryScoreCache	  *cache;
	BlockNumber		   first_segment;
	BlockNumber		   level_heads[TP_MAX_LEVELS];
	bool			   is_partitioned;
	char			  *indexed_colname = NULL;

	/* Get index OID from query */
	index_oid = get_tpquery_index_oid(query);

	/*
	 * Validate query and open index. For partitioned indexes, this opens
	 * the first child index for text config access.
	 */
	index_rel = validate_and_open_index(query, &index_oid);

	/* Check if this is a partitioned index */
	is_partitioned = (get_rel_relkind(index_oid) == RELKIND_PARTITIONED_INDEX);

	/*
	 * Get the indexed column name for child index matching.
	 * We use the column name rather than the raw attnum because
	 * dropped columns can cause attnum drift between parent and
	 * child tables (e.g., TimescaleDB hypertables).
	 */
	{
		HeapTuple	  idx_tuple;
		Form_pg_index idx_form;
		Oid			  lookup_oid;

		lookup_oid = index_oid;
		idx_tuple  = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(lookup_oid));
		if (HeapTupleIsValid(idx_tuple))
		{
			idx_form = (Form_pg_index)GETSTRUCT(idx_tuple);
			if (idx_form->indnatts >= 1)
			{
				AttrNumber attnum = idx_form->indkey.values[0];
				indexed_colname =
						get_attname(idx_form->indrelid, attnum, true);
			}
			ReleaseSysCache(idx_tuple);
		}
	}

	PG_TRY();
	{
		/* Get the metapage to extract text_config_oid */
		metap			= tp_get_metapage(index_rel);
		text_config_oid = metap->text_config_oid;
		first_segment	= metap->level_heads[0];
		for (int i = 0; i < TP_MAX_LEVELS; i++)
			level_heads[i] = metap->level_heads[i];

		/*
		 * Get corpus statistics. For partitioned indexes or inheritance
		 * parents, use the first child's stats as an approximation.
		 */
		if (is_partitioned)
		{
			Oid child_index_oid = RelationGetRelid(index_rel);

			/*
			 * Partitioned indexes have no storage. Use the first child's
			 * stats as an approximation. The child index is the one we
			 * already opened for text config access.
			 */
			index_state = tp_get_local_index_state(child_index_oid);
			if (!index_state)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("could not get index state for partition "
								"index OID %u",
								child_index_oid)));
			}

			total_docs	= pg_atomic_read_u32(&index_state->shared->total_docs);
			total_len	= pg_atomic_read_u64(&index_state->shared->total_len);
			avg_doc_len = total_docs > 0 ? (float4)((double)total_len /
													(double)total_docs)
										 : 0.0f;
		}
		else
		{
			/* Get index state for corpus statistics */
			index_state = tp_get_local_index_state(index_oid);
			if (!index_state)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("could not get index state for index OID %u",
								index_oid)));
			}

			total_docs = pg_atomic_read_u32(&index_state->shared->total_docs);
			total_len  = pg_atomic_read_u64(&index_state->shared->total_len);

			/*
			 * If the index has no documents, check if this is an
			 * inheritance parent (e.g., hypertable) and use the first
			 * child's stats and segments for scoring.
			 */
			if (total_docs == 0 && indexed_colname != NULL)
			{
				Oid first_child_idx = find_first_child_bm25_index(
						index_oid, indexed_colname);

				if (OidIsValid(first_child_idx))
				{
					TpLocalIndexState *child_state = tp_get_local_index_state(
							first_child_idx);
					if (child_state && child_state->shared)
					{
						Relation		child_rel;
						TpIndexMetaPage child_metap;

						index_state = child_state;
						total_docs	= pg_atomic_read_u32(
								 &child_state->shared->total_docs);
						total_len = pg_atomic_read_u64(
								&child_state->shared->total_len);

						/*
						 * Also switch to the child index for segment access.
						 * The parent index has no segments, so IDF lookup
						 * would fail without this.
						 */
						child_rel =
								index_open(first_child_idx, AccessShareLock);
						child_metap	  = tp_get_metapage(child_rel);
						first_segment = child_metap->level_heads[0];
						for (int i = 0; i < TP_MAX_LEVELS; i++)
							level_heads[i] = child_metap->level_heads[i];

						/* Close parent and switch to child relation */
						pfree(metap);
						metap = child_metap;
						index_close(index_rel, AccessShareLock);
						index_rel = child_rel;
					}
				}
			}

			avg_doc_len = total_docs > 0 ? (float4)((double)total_len /
													(double)total_docs)
										 : 0.0f;
		}

		/*
		 * Get or initialize the IDF cache. The cache is stored in fn_extra
		 * and persists for the duration of the query. We invalidate it if
		 * the index state changes (new segments, new documents).
		 */
		cache = (QueryScoreCache *)fcinfo->flinfo->fn_extra;
		if (!cache_is_valid(cache, index_oid, first_segment, total_docs))
		{
			/* Allocate new cache in function memory context */
			cache = (QueryScoreCache *)MemoryContextAllocZero(
					fcinfo->flinfo->fn_mcxt, sizeof(QueryScoreCache));
			cache->index_oid		 = index_oid;
			cache->first_segment	 = first_segment;
			cache->total_docs		 = total_docs;
			cache->avg_doc_len		 = avg_doc_len;
			cache->num_terms		 = 0;
			fcinfo->flinfo->fn_extra = cache;
		}

		/* Normalize and tokenize the document text */
		text_arg = tp_normalize_markup(
				text_arg, (TpContentFormat)tp_metapage_field_format(metap, 0));
		tsvector_datum = DirectFunctionCall2Coll(
				to_tsvector_byid,
				InvalidOid, /* collation */
				ObjectIdGetDatum(text_config_oid),
				PointerGetDatum(text_arg));

		tsvector = DatumGetTSVector(tsvector_datum);

		/*
		 * Calculate document length with fieldnorm quantization.
		 * We use the same encode/decode as segments to ensure the operator
		 * produces identical scores to index scans. This quantization is
		 * a deliberate approximation following Lucene's SmallFloat scheme.
		 */
		doc_length = (float4)decode_fieldnorm(
				encode_fieldnorm((int32)calculate_doc_length(tsvector)));

		/*
		 * Grammar-extension fast path: parse query, dictionary-expand
		 * any prefix terms, and score against the resolved concrete-term
		 * list. Bypasses the tsvector path which can't represent '*'.
		 */
		if (tpquery_is_fuzzy(query) || tpquery_is_grammar(query) ||
			tp_metapage_is_multi_col(metap))
		{
			TpParsedQuery *pq				= tp_parse_query(query_text);
			char		 **resolved_terms	= NULL;
			int32		  *resolved_freqs	= NULL;
			float4		  *resolved_weights = NULL;
			int			   resolved_count;
			int			   r_i;
			bool		   phrase_failed = false;

			if (tpquery_is_fuzzy(query) && tp_parsed_query_has_phrase(pq))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("fuzzy phrase queries are not supported yet"),
						 errhint("Use bm25_fuzzy() with unquoted terms, or "
								 "use to_bm25query() for exact phrases.")));

			/*
			 * Phrase verification first: if any phrase clause fails,
			 * this doc isn't a match — return non-match score.
			 */
			if (tp_parsed_query_has_phrase(pq))
			{
				int	  p_i;
				char *doc_cstr = text_to_cstring(text_arg);

				for (p_i = 0; p_i < pq->term_count && !phrase_failed; p_i++)
				{
					TpQueryTerm	  *qt = &pq->terms[p_i];
					TpPhraseResult vr;
					bool		   last_is_prefix;

					if (qt->kind != TP_QTERM_PHRASE &&
						qt->kind != TP_QTERM_PHRASE_PREFIX)
						continue;

					last_is_prefix = (qt->kind == TP_QTERM_PHRASE_PREFIX);
					vr			   = tp_phrase_verify_text(
							doc_cstr,
							text_config_oid,
							qt->phrase_tokens,
							qt->phrase_token_count,
							last_is_prefix,
							tp_metapage_field_format(metap, 0));
					if (vr != TP_PHRASE_MATCH)
						phrase_failed = true;
				}
				pfree(doc_cstr);
			}

			if (phrase_failed)
			{
				tp_free_parsed_query(pq);
				pfree(metap);
				metap = NULL;
				index_close(index_rel, AccessShareLock);
				index_rel = NULL;
				/* Non-match: return 0 (positive 0 sorts after negatives). */
				PG_RETURN_FLOAT8(0.0);
			}

			resolved_count = tp_resolve_query_terms_ex(
					pq,
					index_state,
					index_rel,
					level_heads,
					text_config_oid,
					metap,
					tpquery_is_fuzzy(query),
					tpquery_fuzzy_max_distance(query),
					&resolved_terms,
					&resolved_freqs,
					tpquery_is_fuzzy(query) ? &resolved_weights : NULL);

			for (r_i = 0; r_i < resolved_count; r_i++)
			{
				char		  *term	  = resolved_terms[r_i];
				int			   tlen	  = (int)strlen(term);
				int32		   q_freq = resolved_freqs[r_i];
				float4		   tf;
				float4		   idf;
				float4		   term_score;
				float4		   term_doc_length = doc_length;
				TpPostingList *posting_list;
				unsigned char  first_byte	 = (unsigned char)term[0];
				int			   tag_field_idx = -1;

				if (first_byte >= TP_FIELD_TAG_BASE)
					tag_field_idx = first_byte - TP_FIELD_TAG_BASE;

				/*
				 * Phase 6.1d: when scoring a tagged term and we have
				 * per-field tsvectors stashed by the record-LHS path,
				 * find tf in that field's tsvector and use that field's
				 * length.  Otherwise fall back to the flattened tsvector
				 * (current behavior, an approximation).
				 */
				if (tag_field_idx >= 0 && tp_bm25f_doc_ctx != NULL &&
					tag_field_idx < tp_bm25f_doc_ctx->num_fields &&
					tp_bm25f_doc_ctx->field_tsvectors[tag_field_idx] != NULL)
				{
					tf = find_term_frequency_by_text(
							tp_bm25f_doc_ctx->field_tsvectors[tag_field_idx],
							term,
							tlen);
					term_doc_length =
							tp_bm25f_doc_ctx->field_lengths[tag_field_idx];
					if (term_doc_length <= 0)
						term_doc_length = 1;
				}
				else
				{
					tf = find_term_frequency_by_text(tsvector, term, tlen);
				}
				if (tf == 0.0f)
					continue;

				{
					uint32 cached_doc_freq = 0;
					idf = lookup_cached_idf(cache, term, &cached_doc_freq);

					if (idf < 0.0f)
					{
						uint32 unified_doc_freq	 = 0;
						uint32 memtable_doc_freq = 0;
						uint32 segment_doc_freq	 = 0;

						posting_list = tp_get_posting_list(index_state, term);
						if (posting_list && posting_list->doc_count > 0)
							memtable_doc_freq = posting_list->doc_count;

						for (int level = 0; level < TP_MAX_LEVELS; level++)
						{
							if (level_heads[level] != InvalidBlockNumber)
							{
								segment_doc_freq += tp_segment_get_doc_freq(
										index_rel, level_heads[level], term);
							}
						}

						unified_doc_freq = memtable_doc_freq +
										   segment_doc_freq;
						if (unified_doc_freq == 0)
							continue;

						idf = tp_calculate_idf(unified_doc_freq, total_docs);
						cache_term_idf(cache, term, unified_doc_freq, idf);
					}
					if (resolved_weights != NULL)
						idf *= resolved_weights[r_i];
				}

				{
					float4 term_avgdl = avg_doc_len;

					if (tag_field_idx >= 0)
					{
						float4 w =
								tp_metapage_field_weight(metap, tag_field_idx);
						idf *= w;
						term_avgdl = tp_metapage_field_avgdl(
								metap, tag_field_idx, avg_doc_len);
					}

					term_score = calculate_term_score(
							tf,
							idf,
							term_doc_length,
							term_avgdl,
							metap->k1,
							metap->b,
							q_freq);
					result += term_score;
				}
			}

			for (r_i = 0; r_i < resolved_count; r_i++)
				pfree(resolved_terms[r_i]);
			if (resolved_terms)
				pfree(resolved_terms);
			if (resolved_freqs)
				pfree(resolved_freqs);
			if (resolved_weights)
				pfree(resolved_weights);
			tp_free_parsed_query(pq);

			query_term_count = 0; /* skip tsvector loop below */
		}
		else
		{
			/* Tokenize the query text to get query terms */
			query_tsvector_datum = DirectFunctionCall2Coll(
					to_tsvector_byid,
					InvalidOid,
					ObjectIdGetDatum(text_config_oid),
					PointerGetDatum(cstring_to_text(query_text)));

			query_tsvector		= DatumGetTSVector(query_tsvector_datum);
			query_entries		= ARRPTR(query_tsvector);
			query_lexemes_start = STRPTR(query_tsvector);

			query_term_count = query_tsvector->size;
		}

		/* Calculate BM25 score for each query term */
		for (q_i = 0; q_i < query_term_count; q_i++)
		{
			char *query_lexeme_raw = query_lexemes_start +
									 query_entries[q_i].pos;
			int			   lexeme_len = query_entries[q_i].len;
			char		  *query_lexeme;
			TpPostingList *posting_list;
			float4		   idf;
			float4		   tf; /* term frequency in document */
			float4		   term_score;
			int			   query_freq;

			/* Create properly null-terminated string from tsvector lexeme */
			query_lexeme = palloc(lexeme_len + 1);
			memcpy(query_lexeme, query_lexeme_raw, lexeme_len);
			query_lexeme[lexeme_len] = '\0';

			if (query_entries[q_i].haspos)
				query_freq = (int32)
						POSDATALEN(query_tsvector, &query_entries[q_i]);
			else
				query_freq = 1;

			/* Find term frequency in the document */
			tf = find_term_frequency(
					tsvector, &query_entries[q_i], query_lexeme);

			if (tf == 0.0f)
			{
				pfree(query_lexeme);
				continue; /* Query term not in document */
			}

			/*
			 * Get IDF from cache if available. The cache avoids repeated
			 * segment opens which was causing catastrophic performance
			 * (opening all segments for each term for each row).
			 */
			{
				uint32 cached_doc_freq = 0;
				idf = lookup_cached_idf(cache, query_lexeme, &cached_doc_freq);

				if (idf < 0.0f)
				{
					/* Cache miss - calculate IDF and cache it */
					uint32 unified_doc_freq	 = 0;
					uint32 memtable_doc_freq = 0;
					uint32 segment_doc_freq	 = 0;

					posting_list =
							tp_get_posting_list(index_state, query_lexeme);
					if (posting_list && posting_list->doc_count > 0)
						memtable_doc_freq = posting_list->doc_count;

					/* Get doc_freq from all segment levels */
					for (int level = 0; level < TP_MAX_LEVELS; level++)
					{
						if (level_heads[level] != InvalidBlockNumber)
						{
							segment_doc_freq += tp_segment_get_doc_freq(
									index_rel,
									level_heads[level],
									query_lexeme);
						}
					}

					unified_doc_freq = memtable_doc_freq + segment_doc_freq;
					if (unified_doc_freq == 0)
					{
						pfree(query_lexeme);
						continue;
					}

					/* Calculate IDF and cache it */
					idf = tp_calculate_idf(unified_doc_freq, total_docs);
					cache_term_idf(cache, query_lexeme, unified_doc_freq, idf);
				}
			}

			/* Calculate BM25 term score */
			term_score = calculate_term_score(
					tf,
					idf,
					doc_length,
					avg_doc_len,
					metap->k1,
					metap->b,
					query_freq);

			/* Accumulate the score */
			result += term_score;

			/* Free the null-terminated string copy */
			pfree(query_lexeme);
		}

		/* Clean up */
		pfree(metap);
		metap = NULL;
		index_close(index_rel, AccessShareLock);
		index_rel = NULL;
	}
	PG_CATCH();
	{
		if (metap)
			pfree(metap);
		if (index_rel)
			index_close(index_rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Return negative score for PostgreSQL ASC ordering compatibility */
	PG_RETURN_FLOAT8((result > 0) ? -result : result);
}

/*
 * tpquery equality function
 */
Datum
tpquery_eq(PG_FUNCTION_ARGS)
{
	TpQuery *a = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	TpQuery *b = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	/* Compare index OIDs */
	if (a->index_oid != b->index_oid)
		PG_RETURN_BOOL(false);

	if (a->flags != b->flags)
		PG_RETURN_BOOL(false);

	if (tpquery_is_fuzzy(a) &&
		tpquery_fuzzy_max_distance(a) != tpquery_fuzzy_max_distance(b))
		PG_RETURN_BOOL(false);

	/* Compare query text lengths */
	if (a->query_text_len != b->query_text_len)
		PG_RETURN_BOOL(false);

	/* Compare query texts */
	{
		char *query_text_a = get_tpquery_text(a);
		char *query_text_b = get_tpquery_text(b);

		if (strcmp(query_text_a, query_text_b) != 0)
			PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Utility function to create a tpquery with resolved index OID and options.
 */
TpQuery *
create_tpquery_options(
		const char *query_text,
		Oid			index_oid,
		bool		explicit_index,
		bool		grammar,
		bool		fuzzy,
		uint8		fuzzy_max_distance)
{
	TpQuery *result;
	int		 query_text_len = strlen(query_text);
	int		 total_size;

	total_size = offsetof(TpQuery, data) + query_text_len + 1;

	result = (TpQuery *)palloc0(total_size);
	SET_VARSIZE(result, total_size);
	result->version = TPQUERY_VERSION;
	result->flags	= 0;
	if (explicit_index)
		result->flags |= TPQUERY_FLAG_EXPLICIT_INDEX;
	if (grammar)
		result->flags |= TPQUERY_FLAG_GRAMMAR;
	if (fuzzy)
		result->flags |= TPQUERY_FLAG_FUZZY;
	result->fuzzy_max_distance = fuzzy ? fuzzy_max_distance : 0;
	result->index_oid		   = index_oid;
	result->query_text_len	   = query_text_len;

	memcpy(result->data, query_text, query_text_len);
	result->data[query_text_len] = '\0';

	return result;
}

TpQuery *
create_tpquery_explicit(
		const char *query_text, Oid index_oid, bool explicit_index)
{
	return create_tpquery_options(
			query_text, index_oid, explicit_index, false, false, 0);
}

/*
 * Utility function to create a tpquery with resolved index OID.
 * Index is marked as NOT explicit (for implicit resolution).
 */
TpQuery *
create_tpquery(const char *query_text, Oid index_oid)
{
	return create_tpquery_explicit(query_text, index_oid, false);
}

/*
 * Create a tpquery from index name (resolves name to OID).
 * Index is marked as EXPLICIT since user provided the name.
 *
 * Partitioned indexes are allowed - they will be resolved to the
 * appropriate partition index at scan time.
 */
TpQuery *
create_tpquery_from_name(const char *query_text, const char *index_name)
{
	return create_tpquery_from_name_options(
			query_text, index_name, false, false, 0);
}

TpQuery *
create_tpquery_from_name_options(
		const char *query_text,
		const char *index_name,
		bool		grammar,
		bool		fuzzy,
		uint8		fuzzy_max_distance)
{
	Oid	 index_oid		= InvalidOid;
	bool explicit_index = false;

	if (index_name != NULL)
	{
		index_oid = tp_resolve_index_name_shared(index_name);
		if (!OidIsValid(index_oid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("index \"%s\" does not exist", index_name)));
		}
		explicit_index = true;
	}

	return create_tpquery_options(
			query_text,
			index_oid,
			explicit_index,
			grammar,
			fuzzy,
			fuzzy_max_distance);
}

/*
 * Get index OID from tpquery (returns InvalidOid if unresolved)
 */
Oid
get_tpquery_index_oid(TpQuery *tpquery)
{
	return tpquery->index_oid;
}

/*
 * Get query text from tpquery
 */
char *
get_tpquery_text(TpQuery *tpquery)
{
	return TPQUERY_TEXT_PTR(tpquery);
}

/*
 * Check if tpquery has a resolved index
 */
bool
tpquery_has_index(TpQuery *tpquery)
{
	return OidIsValid(tpquery->index_oid);
}

/*
 * Check if tpquery has an explicitly specified index.
 * Returns true if the user specified an index via to_bm25query(text,
 * index_name) or bm25query('index_name:text').
 * Returns false for implicitly resolved indexes.
 */
bool
tpquery_is_explicit_index(TpQuery *tpquery)
{
	/* Version 1 didn't have flags, treat as non-explicit */
	if (tpquery->version < 2)
		return false;
	return (tpquery->flags & TPQUERY_FLAG_EXPLICIT_INDEX) != 0;
}

bool
tpquery_is_grammar(TpQuery *tpquery)
{
	if (tpquery->version < 3)
		return false;
	return (tpquery->flags & TPQUERY_FLAG_GRAMMAR) != 0;
}

bool
tpquery_is_fuzzy(TpQuery *tpquery)
{
	if (tpquery->version < 3)
		return false;
	return (tpquery->flags & TPQUERY_FLAG_FUZZY) != 0;
}

uint8
tpquery_fuzzy_max_distance(TpQuery *tpquery)
{
	if (!tpquery_is_fuzzy(tpquery))
		return 0;
	return tpquery->fuzzy_max_distance;
}

/*
 * Scoring function for text <@> text operations.
 *
 * This function should never actually be called because the planner hook
 * transforms all `text <@> text` expressions to `text <@> bm25query`.
 * We keep this as a fallback that errors with a helpful message.
 */
Datum
bm25_text_text_score(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("no BM25 index found for text <@> text expression"),
			 errdetail("Create a BM25 index on the column or use ORDER BY."),
			 errhint("SELECT col <@> to_bm25query('q', 'idx') AS score")));

	PG_RETURN_NULL(); /* never reached */
}

/*
 * Standalone scoring for text[] <@> bm25query.
 *
 * Flattens the array elements into a single space-separated text
 * value, then delegates to bm25_text_bm25query_score().
 */
Datum
bm25_textarray_bm25query_score(PG_FUNCTION_ARGS)
{
	Datum array_datum = PG_GETARG_DATUM(0);
	text *flattened	  = tp_flatten_text_array(array_datum);

	/*
	 * Replace the first argument with the flattened text,
	 * then delegate to the scalar scoring function.
	 */
	fcinfo->args[0].value  = PointerGetDatum(flattened);
	fcinfo->args[0].isnull = false;

	return bm25_text_bm25query_score(fcinfo);
}

/*
 * Standalone scoring for record <@> bm25query.
 *
 * The planner hook normally rewrites `(a, b) <@> q` to use the
 * matching multi-col BM25 index for an exact BM25F index scan.
 * This function is the fallback when the hook can't rewrite (no
 * matching index, seq scan forced, etc.).
 *
 * Phase 6.1d: instead of flattening the record into one text and
 * losing the per-column boundary, we deform the tuple, tokenize each
 * text-like column separately, and stash the per-field tsvectors +
 * fieldnorm-quantized lengths in tp_bm25f_doc_ctx.  The text scorer's
 * grammar path then pulls tf and doc length from the right field for
 * each tagged term, so the standalone scores match the index scan
 * scores at the BM25F formula level.
 *
 * fcinfo->args[0] still gets a flattened text so untagged terms
 * (rare in multi-col, but possible for legacy single-col indexes
 * accessed via record LHS) score consistently with prior behavior.
 */
Datum
bm25_record_bm25query_score(PG_FUNCTION_ARGS)
{
	Datum			record_datum = PG_GETARG_DATUM(0);
	text		   *flattened	 = tp_flatten_record(record_datum);
	HeapTupleHeader rec;
	Oid				tupType;
	int32			tupTypmod;
	TupleDesc		tupdesc;
	HeapTupleData	tuple;
	TpBm25fDocCtx	ctx;
	TpBm25fDocCtx  *saved;
	TpQuery		   *query;
	Oid				text_config_oid = InvalidOid;
	Datum			result;
	int				col;
	uint8			rec_field_formats[TP_MAX_FIELDS];

	memset(&ctx, 0, sizeof(ctx));
	memset(rec_field_formats, 0, sizeof(rec_field_formats));

	/*
	 * Look up the bm25query's index to fetch text_config_oid for
	 * tokenization.  We do this without taking a heavy lock; the
	 * inner bm25_text_bm25query_score reopens the index properly.
	 */
	query = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
	{
		Oid				index_oid;
		Relation		index_rel;
		TpIndexMetaPage mp;

		index_oid = get_tpquery_index_oid(query);
		index_rel = validate_and_open_index(query, &index_oid);

		if (index_rel != NULL)
		{
			mp				= tp_get_metapage(index_rel);
			text_config_oid = mp->text_config_oid;
			ctx.num_fields	= mp->num_fields;
			memcpy(rec_field_formats,
				   mp->field_formats,
				   sizeof(rec_field_formats));
			pfree(mp);
			index_close(index_rel, AccessShareLock);
		}
	}

	/*
	 * Deform the record and tokenize each text-like column.  Skip
	 * the per-field path entirely if the index isn't multi-col —
	 * standalone scoring then behaves identically to the text-LHS
	 * scorer on a flattened input.
	 */
	if (ctx.num_fields > 1 && OidIsValid(text_config_oid))
	{
		rec		  = DatumGetHeapTupleHeader(record_datum);
		tupType	  = HeapTupleHeaderGetTypeId(rec);
		tupTypmod = HeapTupleHeaderGetTypMod(rec);
		tupdesc	  = lookup_rowtype_tupdesc(tupType, tupTypmod);

		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_len		 = HeapTupleHeaderGetDatumLength(rec);
		tuple.t_data	 = rec;

		for (col = 0; col < tupdesc->natts && col < ctx.num_fields &&
					  col < TP_MAX_FIELDS;
			 col++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, col);
			Datum			  val;
			bool			  isnull;
			Oid				  typid = att->atttypid;
			text			 *coltext;
			Datum			  tsvec_datum;
			TSVector		  tsvec;

			if (att->attisdropped)
				continue;

			val = heap_getattr(&tuple, col + 1, tupdesc, &isnull);
			if (isnull)
				continue;

			if (typid == TEXTOID || typid == VARCHAROID || typid == BPCHAROID)
				coltext = DatumGetTextPP(val);
			else if (tp_is_text_array_type(typid))
				coltext = tp_flatten_text_array(val);
			else
				continue;

			coltext = tp_normalize_markup(
					coltext, (TpContentFormat)rec_field_formats[col]);

			tsvec_datum = DirectFunctionCall2Coll(
					to_tsvector_byid,
					InvalidOid,
					ObjectIdGetDatum(text_config_oid),
					PointerGetDatum(coltext));
			tsvec					 = DatumGetTSVector(tsvec_datum);
			ctx.field_tsvectors[col] = tsvec;
			ctx.field_lengths[col]	 = (int32)decode_fieldnorm(
					  encode_fieldnorm((int32)calculate_doc_length(tsvec)));
		}

		ReleaseTupleDesc(tupdesc);
	}

	saved			 = tp_bm25f_doc_ctx;
	tp_bm25f_doc_ctx = (ctx.num_fields > 1) ? &ctx : NULL;

	fcinfo->args[0].value  = PointerGetDatum(flattened);
	fcinfo->args[0].isnull = false;

	PG_TRY();
	{
		result = bm25_text_bm25query_score(fcinfo);
	}
	PG_FINALLY();
	{
		tp_bm25f_doc_ctx = saved;
	}
	PG_END_TRY();

	return result;
}

/*
 * Error stub for text[] <@> text when planner rewrite fails.
 */
Datum
bm25_textarray_text_score(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("no BM25 index found for text[] <@> text "
					"expression"),
			 errdetail(
					 "Create a BM25 index on the column or "
					 "use ORDER BY."),
			 errhint("SELECT col <@> to_bm25query('q', 'idx') "
					 "AS score")));

	PG_RETURN_NULL(); /* never reached */
}
