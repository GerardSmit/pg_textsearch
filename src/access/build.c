/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * build.c - BM25 index build, insert, and spill operations
 */
#include <postgres.h>

#include <access/generic_xlog.h>
#include <access/tableam.h>
#include <catalog/namespace.h>
#include <catalog/storage.h>
#include <commands/progress.h>
#include <executor/spi.h>
#include <math.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <optimizer/optimizer.h>
#include <storage/bufmgr.h>
#include <tsearch/ts_type.h>
#include <utils/acl.h>
#include <utils/backend_progress.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/regproc.h>

#include "access/am.h"
#include "access/build_context.h"
#include "access/build_parallel.h"
#include "constants.h"
#include "index/metapage.h"
#include "index/registry.h"
#include "index/state.h"
#include "memtable/memtable.h"
#include "memtable/posting.h"
#include "memtable/stringtable.h"
#include "segment/io.h"
#include "segment/merge.h"
#include "segment/segment.h"
#include "types/array.h"
#include "types/markup.h"
#include "types/vector.h"

/*
 * Build progress tracking for partitioned tables.
 *
 * When creating a BM25 index on a partitioned table, tp_build()
 * is called once per partition. Without tracking, each call emits
 * repeated NOTICE messages, producing many lines of noise. This
 * state aggregates statistics across partitions and emits a
 * single summary.
 *
 * Activated by ProcessUtility_hook in mod.c when it detects
 * CREATE INDEX USING bm25.
 */
static struct
{
	bool   active;
	int	   partition_count;
	uint64 total_docs;
	uint64 total_len;
} build_progress;

void
tp_build_progress_begin(void)
{
	memset(&build_progress, 0, sizeof(build_progress));
	build_progress.active = true;
}

void
tp_build_progress_end(void)
{
	double avg_len = 0.0;

	if (!build_progress.active)
		return;

	build_progress.active = false;

	if (build_progress.total_docs > 0)
		avg_len = (double)build_progress.total_len /
				  (double)build_progress.total_docs;

	if (build_progress.partition_count > 1)
		elog(NOTICE,
			 "BM25 index build completed: " UINT64_FORMAT
			 " documents across %d partitions,"
			 " avg_length=%.2f",
			 build_progress.total_docs,
			 build_progress.partition_count,
			 avg_len);
	else
		elog(NOTICE,
			 "BM25 index build completed: " UINT64_FORMAT
			 " documents, avg_length=%.2f",
			 build_progress.total_docs,
			 avg_len);
}

/*
 * Build phase name for progress reporting
 */
char *
tp_buildphasename(int64 phase)
{
	switch (phase)
	{
	case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
		return "initializing";
	case TP_PHASE_LOADING:
		return "loading tuples";
	case TP_PHASE_WRITING:
		return "writing index";
	case TP_PHASE_COMPACTING:
		return "compacting segments";
	default:
		return NULL;
	}
}

/*
 * Spill the current index's memtable to a disk segment.
 * Returns true if a segment was written.
 *
 * Caller must already hold LW_EXCLUSIVE on the per-index lock.
 */
static bool
tp_do_spill(TpLocalIndexState *index_state, Relation index_rel)
{
	BlockNumber root;

	root = tp_write_segment(index_state, index_rel);
	if (root == InvalidBlockNumber)
		return false;

	tp_clear_memtable(index_state);
	tp_clear_docid_pages(index_rel);
	tp_link_l0_chain_head(index_rel, root);
	tp_sync_metapage_stats(index_rel, index_state);

	pgstat_progress_update_param(
			PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_COMPACTING);
	tp_maybe_compact_level(index_rel, 0);
	pgstat_progress_update_param(
			PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_LOADING);

	return true;
}

/*
 * Auto-spill memtable when memory limits are exceeded.
 *
 * Checks in order:
 * 1. Legacy memtable_spill_threshold (posting count).
 * 2. Per-index soft limit: memory_limit / 8.
 * 3. Global soft limit: memory_limit / 2
 *    (amortized every ~100 documents).
 *
 * The hard limit (memory_limit itself) is checked separately
 * in tp_check_hard_limit().
 */
static void
tp_auto_spill_if_needed(TpLocalIndexState *index_state, Relation index_rel)
{
	TpMemtable *memtable;
	bool		needs_spill = false;

	if (!index_state || !index_rel || !index_state->shared)
		return;

	memtable = get_memtable(index_state);
	if (!memtable)
		return;

	/*
	 * Check thresholds without the per-index lock.  These reads
	 * are approximate: a concurrent insert may have bumped the
	 * counters since we read them, and a concurrent spill may
	 * have cleared them.  That's fine — a false positive just
	 * triggers an unnecessary lock acquisition (the double-check
	 * under LW_EXCLUSIVE below catches it), and a false negative
	 * means this insert doesn't spill but the next one will.
	 */

	/* Legacy per-index posting count threshold */
	if (tp_memtable_spill_threshold > 0 &&
		pg_atomic_read_u64(&memtable->total_postings) >=
				(uint64)tp_memtable_spill_threshold)
	{
		needs_spill = true;
	}

	if (!needs_spill && tp_memory_limit > 0)
	{
		/* Per-index soft limit: memory_limit / 8 */
		uint64 limit = tp_per_index_limit_bytes();
		uint64 est	 = tp_estimate_memtable_bytes(memtable);

		/* Update global estimate while we have it */
		tp_update_index_estimate(index_state->shared, memtable);

		if (limit > 0 && est > limit)
			needs_spill = true;

		/*
		 * Global soft limit: memory_limit / 2
		 * (amortized every ~100 docs)
		 */
		if (!needs_spill && ++index_state->docs_since_global_check >= 100)
		{
			uint64 g_limit = tp_soft_limit_bytes();
			uint64 g_est;

			index_state->docs_since_global_check = 0;
			g_est = tp_get_estimated_total_bytes();

			if (g_est > g_limit)
			{
				/*
				 * Global memory exceeds the soft limit.
				 * Try to evict the largest memtable.  This
				 * can fail if every candidate is locked by
				 * another backend or has an empty memtable
				 * (stale estimate).
				 */
				if (!tp_evict_largest_memtable(index_state->shared->index_oid))
				{
					elog(WARNING,
						 "pg_textsearch: global soft "
						 "limit exceeded "
						 "(" UINT64_FORMAT " kB / " UINT64_FORMAT
						 " kB) but no "
						 "memtable could be spilled",
						 (uint64)(g_est / 1024),
						 (uint64)(g_limit / 1024));
				}
				return;
			}
		}
	}

	if (!needs_spill)
		return;

	/*
	 * Acquire exclusive lock to spill.  This blocks concurrent
	 * inserters (who hold LW_SHARED) until the spill completes.
	 * The spill itself writes segment pages and updates the
	 * metapage — it does not acquire any other LWLock or
	 * heavyweight lock, so deadlock is not possible.  The
	 * duration is bounded by the memtable size (limited by the
	 * per-index soft limit).
	 */
	tp_acquire_index_lock(index_state, LW_EXCLUSIVE);

	/*
	 * Re-check: another backend may have spilled while
	 * we waited for the exclusive lock.
	 */
	memtable = get_memtable(index_state);
	if (memtable)
	{
		bool still_needed = false;

		if (tp_memtable_spill_threshold > 0 &&
			pg_atomic_read_u64(&memtable->total_postings) >=
					(uint64)tp_memtable_spill_threshold)
			still_needed = true;

		if (!still_needed && tp_memory_limit > 0)
		{
			uint64 limit = tp_per_index_limit_bytes();
			uint64 est	 = tp_estimate_memtable_bytes(memtable);
			if (limit > 0 && est > limit)
				still_needed = true;
		}

		if (still_needed)
			tp_do_spill(index_state, index_rel);
	}

	tp_release_index_lock(index_state);
}

/*
 * Hard limit check: fail the current operation if DSA segment
 * memory exceeds memory_limit.  Called from tp_insert before
 * adding terms to the memtable.
 *
 * Raises ERROR if the limit is exceeded; returns normally
 * otherwise.
 */
static void
tp_check_hard_limit(void)
{
	uint64 limit_bytes;
	uint64 dsa_bytes;

	if (tp_memory_limit <= 0)
		return;

	limit_bytes = tp_hard_limit_bytes();

	tp_registry_update_dsa_counter();
	dsa_bytes = tp_registry_get_total_dsa_bytes();

	if (dsa_bytes > limit_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_textsearch DSA memory "
						"(" UINT64_FORMAT " kB) exceeds "
						"memory_limit "
						"(%d kB)",
						(uint64)(dsa_bytes / 1024),
						tp_memory_limit),
				 errhint("Increase pg_textsearch."
						 "memory_limit or spill "
						 "indexes with "
						 "bm25_spill_index().")));
}

/*
 * Flush build context to a segment and link as L0 chain head.
 * Used during serial CREATE INDEX with arena-based build.
 */
static void
tp_build_flush_and_link(TpBuildContext *ctx, Relation index)
{
	BlockNumber segment_root;

	segment_root = tp_write_segment_from_build_ctx(ctx, index);
	if (segment_root == InvalidBlockNumber)
		return;

	tp_link_l0_chain_head(index, segment_root);
}

/*
 * Link a newly-written segment as the L0 chain head.
 *
 * Reads the metapage, points the new segment's next_segment at the
 * current L0 head, then updates the metapage head and count.
 */
void
tp_link_l0_chain_head(Relation index, BlockNumber segment_root)
{
	Buffer			  metabuf;
	Buffer			  seg_buf = InvalidBuffer;
	GenericXLogState *state;
	Page			  metapage;
	TpIndexMetaPage	  metap;

	metabuf = ReadBuffer(index, TP_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	state	 = GenericXLogStart(index);
	metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
	metap	 = (TpIndexMetaPage)PageGetContents(metapage);

	if (metap->level_heads[0] != InvalidBlockNumber)
	{
		Page			 seg_page;
		TpSegmentHeader *seg_header;

		seg_buf = ReadBuffer(index, segment_root);
		LockBuffer(seg_buf, BUFFER_LOCK_EXCLUSIVE);
		seg_page = GenericXLogRegisterBuffer(state, seg_buf, 0);
		/* Ensure pd_lower covers content for GenericXLog */
		((PageHeader)seg_page)->pd_lower = BLCKSZ;
		seg_header = (TpSegmentHeader *)PageGetContents(seg_page);
		seg_header->next_segment = metap->level_heads[0];
	}

	metap->level_heads[0] = segment_root;
	metap->level_counts[0]++;

	GenericXLogFinish(state);
	if (BufferIsValid(seg_buf))
		UnlockReleaseBuffer(seg_buf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Truncate dead pages from an index relation.
 *
 * Walks all segment chains via the metapage to find the highest
 * block still in use, then truncates everything beyond it.
 * This reclaims pages freed by compaction (which sit below the
 * high-water mark) and unused pool margin from parallel builds.
 */
void
tp_truncate_dead_pages(Relation index)
{
	Buffer			metabuf;
	Page			metapage;
	TpIndexMetaPage metap;
	BlockNumber		max_used = 1; /* at least metapage */
	BlockNumber		nblocks;
	int				level;

	metabuf = ReadBuffer(index, TP_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	metap	 = (TpIndexMetaPage)PageGetContents(metapage);

	for (level = 0; level < TP_MAX_LEVELS; level++)
	{
		BlockNumber seg = metap->level_heads[level];

		while (seg != InvalidBlockNumber)
		{
			TpSegmentReader *reader;
			BlockNumber		*pages;
			uint32			 num_pages;
			uint32			 i;

			num_pages = tp_segment_collect_pages(index, seg, &pages);
			for (i = 0; i < num_pages; i++)
			{
				if (pages[i] + 1 > max_used)
					max_used = pages[i] + 1;
			}
			if (pages)
				pfree(pages);

			reader = tp_segment_open(index, seg);
			seg	   = reader->header->next_segment;
			tp_segment_close(reader);
		}
	}

	UnlockReleaseBuffer(metabuf);

	nblocks = RelationGetNumberOfBlocks(index);
	if (max_used < nblocks)
		RelationTruncate(index, max_used);
}

/*
 * tp_spill_memtable - Force memtable flush to disk segment
 *
 * This function allows manual triggering of segment writes.
 * Returns the block number of the written segment, or NULL if memtable was
 * empty.
 */
PG_FUNCTION_INFO_V1(tp_spill_memtable);

Datum
tp_spill_memtable(PG_FUNCTION_ARGS)
{
	text			  *index_name_text = PG_GETARG_TEXT_PP(0);
	char			  *index_name	   = text_to_cstring(index_name_text);
	Oid				   index_oid;
	Relation		   index_rel;
	TpLocalIndexState *index_state;
	BlockNumber		   segment_root;
	RangeVar		  *rv;

	/* Parse index name (supports schema.index notation) */
	rv = makeRangeVarFromNameList(stringToQualifiedNameList(index_name, NULL));
	index_oid = RangeVarGetRelid(rv, AccessShareLock, false);

	if (!OidIsValid(index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" does not exist", index_name)));

	/* Check that caller owns the index */
	if (!object_ownercheck(RelationRelationId, index_oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, index_name);

	/* Open the index */
	index_rel = index_open(index_oid, RowExclusiveLock);

	/* Get index state */
	index_state = tp_get_local_index_state(RelationGetRelid(index_rel));
	if (!index_state)
	{
		index_close(index_rel, RowExclusiveLock);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not get index state for \"%s\"", index_name)));
	}

	/* Acquire exclusive lock for write operation */
	tp_acquire_index_lock(index_state, LW_EXCLUSIVE);

	/* Write the segment */
	segment_root = tp_write_segment(index_state, index_rel);

	/* Clear the memtable after successful spilling */
	if (segment_root != InvalidBlockNumber)
	{
		tp_clear_memtable(index_state);
		tp_clear_docid_pages(index_rel);
		tp_link_l0_chain_head(index_rel, segment_root);
		tp_sync_metapage_stats(index_rel, index_state);

		/* Check if L0 needs compaction */
		tp_maybe_compact_level(index_rel, 0);
	}

	/* Release lock */
	tp_release_index_lock(index_state);

	/* Close the index */
	index_close(index_rel, RowExclusiveLock);

	/* Return block number or NULL */
	if (segment_root != InvalidBlockNumber)
	{
		PG_RETURN_INT32(segment_root);
	}
	else
	{
		PG_RETURN_NULL();
	}
}

PG_FUNCTION_INFO_V1(tp_force_merge);

/*
 * SQL-callable: bm25_force_merge(index_name text) → void
 *
 * Force-merge all segments into a single segment, à la Lucene's
 * forceMerge(1).  Useful after bulk loads or when benchmarking
 * with a single-segment layout.
 */
Datum
tp_force_merge(PG_FUNCTION_ARGS)
{
	text	 *index_name_text = PG_GETARG_TEXT_PP(0);
	char	 *index_name	  = text_to_cstring(index_name_text);
	Oid		  index_oid;
	Relation  index_rel;
	RangeVar *rv;

	rv = makeRangeVarFromNameList(stringToQualifiedNameList(index_name, NULL));
	index_oid = RangeVarGetRelid(rv, AccessShareLock, false);

	if (!OidIsValid(index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" does not exist", index_name)));

	/* Check that caller owns the index */
	if (!object_ownercheck(RelationRelationId, index_oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, index_name);

	index_rel = index_open(index_oid, RowExclusiveLock);
	tp_force_merge_all(index_rel);
	tp_truncate_dead_pages(index_rel);

	index_close(index_rel, RowExclusiveLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(tp_prewarm_index);

/*
 * SQL-callable: bm25_prewarm(index_name text) → int8
 *
 * Walks every page of the index relation via ReadBuffer, populating
 * shared_buffers so subsequent queries hit warm cache. Analogous to
 * contrib/pg_prewarm's pg_prewarm() for heap/indexes but scoped to
 * the pg_textsearch on-disk layout. Returns the number of pages
 * loaded (including any stub/free pages — the caller gets the full
 * on-disk page count).
 *
 * Use case: after a server restart or during warm-up of a
 * read-heavy workload, preload the positions + posting + skip index
 * sections into memory so the first phrase query doesn't pay cold
 * cache latency.
 */
Datum
tp_prewarm_index(PG_FUNCTION_ARGS)
{
	text	   *index_name_text = PG_GETARG_TEXT_PP(0);
	char	   *index_name		= text_to_cstring(index_name_text);
	Oid			index_oid;
	Relation	index_rel;
	RangeVar   *rv;
	BlockNumber nblocks;
	BlockNumber blk;
	int64		loaded = 0;

	rv = makeRangeVarFromNameList(stringToQualifiedNameList(index_name, NULL));
	index_oid = RangeVarGetRelid(rv, AccessShareLock, false);

	if (!OidIsValid(index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" does not exist", index_name)));

	index_rel = index_open(index_oid, AccessShareLock);
	nblocks	  = RelationGetNumberOfBlocks(index_rel);

	for (blk = 0; blk < nblocks; blk++)
	{
		Buffer buf = ReadBuffer(index_rel, blk);
		/*
		 * No lock needed — we just want the page resident in
		 * shared_buffers. Release immediately so the buffer-clock
		 * doesn't see us as a "long-running pin".
		 */
		ReleaseBuffer(buf);
		loaded++;

		/*
		 * Yield to signal handlers every 1024 pages so CTRL-C /
		 * query-cancel works on large indexes. Standard Postgres
		 * pattern.
		 */
		if ((blk & 1023) == 0)
			CHECK_FOR_INTERRUPTS();
	}

	index_close(index_rel, AccessShareLock);
	pfree(index_name);

	PG_RETURN_INT64(loaded);
}

/*
 * Parse a comma-separated decimal list like "3.0,1.0,0.5" into
 * weights[0..expected-1]. Errors on wrong count or non-numeric
 * entries. Trailing/leading whitespace per entry is trimmed.
 */
static void
tp_parse_field_weights(const char *raw, int expected, float4 *out_weights)
{
	const char *p = raw;
	int			i;

	for (i = 0; i < expected; i++)
	{
		char	   *endptr;
		double		v;
		const char *tok_start;

		while (*p == ' ' || *p == '\t')
			p++;
		tok_start = p;

		errno = 0;
		v	  = strtod(p, &endptr);
		if (endptr == p || errno != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not parse field_weights at '%s'",
							tok_start),
					 errhint("Format: 'w0,w1,...' with one positive "
							 "number per indexed column.")));
		if (v <= 0.0 || v > 1e6)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("field_weights entry %d (%g) out of range (>0, "
							"<=1e6)",
							i,
							v)));
		out_weights[i] = (float4)v;
		p			   = endptr;
		while (*p == ' ' || *p == '\t')
			p++;
		if (i < expected - 1)
		{
			if (*p != ',')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("field_weights has fewer entries than "
								"columns (expected %d)",
								expected)));
			p++;
		}
	}

	while (*p == ' ' || *p == '\t' || *p == ',')
		p++;
	if (*p != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field_weights has more entries than columns "
						"(expected %d)",
						expected)));
}

/*
 * Parse content_format option into per-field format array.
 *
 * Bare value (e.g. "markdown") applies to all fields.
 * Per-field syntax: "title:plain,body:markdown".
 */
static void
tp_parse_content_formats(
		const char		  *raw,
		int				   num_fields,
		const char *const *field_names,
		uint8			  *out_formats)
{
	const char *p = raw;

	while (*p == ' ' || *p == '\t')
		p++;

	/* Check if bare value (no colon before first comma/end) */
	{
		const char *scan	  = p;
		bool		has_colon = false;
		while (*scan && *scan != ',')
		{
			if (*scan == ':')
			{
				has_colon = true;
				break;
			}
			scan++;
		}

		if (!has_colon)
		{
			TpContentFormat fmt = tp_parse_content_format(p);
			int				i;
			for (i = 0; i < num_fields; i++)
				out_formats[i] = (uint8)fmt;
			return;
		}
	}

	/* Per-field syntax: "field:format,field:format,..." */
	while (*p)
	{
		char field_name[TP_MAX_FIELD_NAME_LEN + 1];
		char fmt_str[32];
		int	 fi = 0, vi = 0;
		int	 field_idx;

		while (*p == ' ' || *p == '\t' || *p == ',')
			p++;
		if (*p == '\0')
			break;

		while (*p && *p != ':' && fi < TP_MAX_FIELD_NAME_LEN)
			field_name[fi++] = *p++;
		field_name[fi] = '\0';

		if (*p != ':')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid content_format syntax "
							"near \"%s\"",
							field_name),
					 errhint("Use 'field:format,...' or a "
							 "bare format name.")));
		p++;

		while (*p && *p != ',' && vi < 31)
			fmt_str[vi++] = *p++;
		fmt_str[vi] = '\0';

		/* Trim trailing whitespace */
		while (vi > 0 && (fmt_str[vi - 1] == ' ' || fmt_str[vi - 1] == '\t'))
			fmt_str[--vi] = '\0';

		field_idx = -1;
		{
			int i;
			for (i = 0; i < num_fields; i++)
			{
				if (pg_strcasecmp(field_names[i], field_name) == 0)
				{
					field_idx = i;
					break;
				}
			}
		}
		if (field_idx < 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("content_format references "
							"unknown field \"%s\"",
							field_name)));

		out_formats[field_idx] = (uint8)tp_parse_content_format(fmt_str);
	}
}

/*
 * Helper: Extract options from index relation
 */
static void
tp_build_extract_options(
		Relation index,
		char   **text_config_name,
		Oid		*text_config_oid,
		double	*k1,
		double	*b,
		char   **field_weights_str,
		char   **content_format_str)
{
	TpOptions *options;

	*text_config_name = NULL;
	*text_config_oid  = InvalidOid;
	if (field_weights_str != NULL)
		*field_weights_str = NULL;
	if (content_format_str != NULL)
		*content_format_str = NULL;

	/* Extract options from index */
	options = (TpOptions *)index->rd_options;
	if (options)
	{
		if (options->text_config_offset > 0)
		{
			*text_config_name = pstrdup(
					(char *)options + options->text_config_offset);
			/* Convert text config name to OID */
			{
				List *names =
						stringToQualifiedNameList(*text_config_name, NULL);

				*text_config_oid = get_ts_config_oid(names, false);
				list_free(names);
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("text_config parameter is required for bm25 "
							"indexes"),
					 errhint("Specify text_config when creating the index: "
							 "CREATE INDEX ... USING "
							 "bm25(column) WITH (text_config='english')")));
		}

		*k1 = options->k1;
		*b	= options->b;

		if (field_weights_str != NULL && options->field_weights_offset > 0)
		{
			*field_weights_str = pstrdup(
					(char *)options + options->field_weights_offset);
		}
		if (content_format_str != NULL && options->content_format_offset > 0)
		{
			*content_format_str = pstrdup(
					(char *)options + options->content_format_offset);
		}
	}
	else
	{
		/* No options provided - require text_config */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("text_config parameter is required for bm25 indexes"),
				 errhint("Specify text_config when creating the index: "
						 "CREATE INDEX ... USING "
						 "bm25(column) WITH (text_config='english')")));
	}
}

/*
 * Helper: Initialize metapage for new index, optionally with a
 * multi-column field registry.
 *
 * When num_fields >= 2, terms in this index are stored with a leading
 * tag byte identifying which column they came from, and the metapage
 * records each column's name so `field:term` grammar can resolve to
 * the tagged form.
 */
static void
tp_build_init_metapage(
		Relation		   index,
		Oid				   text_config_oid,
		double			   k1,
		double			   b,
		int				   num_fields,
		const char *const *field_names,
		const float4	  *field_weights,
		const uint8		  *field_formats)
{
	Buffer			  metabuf;
	GenericXLogState *state;
	Page			  metapage;
	TpIndexMetaPage	  metap;

	/* Initialize metapage */
	metabuf = ReadBuffer(index, P_NEW);
	Assert(BufferGetBlockNumber(metabuf) == TP_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	metapage =
			GenericXLogRegisterBuffer(state, metabuf, GENERIC_XLOG_FULL_IMAGE);

	tp_init_metapage_multi(
			metapage,
			text_config_oid,
			num_fields,
			field_names,
			field_weights,
			field_formats);
	metap	  = (TpIndexMetaPage)PageGetContents(metapage);
	metap->k1 = k1;
	metap->b  = b;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Extract terms, frequencies, and (optionally) positions from a
 * TSVector. Returns the document length (sum of all term frequencies).
 *
 * For each indexed term, TSVector stores 1-based ordinal positions
 * of that term's occurrences in the source text. When positions_out is
 * non-NULL we capture those ordinals. WEP_GETPOS strips the upper-2-bit
 * weight from each WordEntryPos.
 */
int
tp_extract_terms_from_tsvector(
		TSVector  tsvector,
		char   ***terms_out,
		int32	**frequencies_out,
		uint32 ***positions_out,
		int		 *term_count_out)
{
	int		   term_count = tsvector->size;
	char	 **terms;
	int32	  *frequencies;
	uint32	 **positions  = NULL;
	int		   doc_length = 0;
	int		   i;
	WordEntry *we;

	*term_count_out = term_count;

	if (term_count == 0)
	{
		*terms_out		 = NULL;
		*frequencies_out = NULL;
		if (positions_out)
			*positions_out = NULL;
		return 0;
	}

	we = ARRPTR(tsvector);

	terms		= palloc(term_count * sizeof(char *));
	frequencies = palloc(term_count * sizeof(int32));
	if (positions_out)
		positions = palloc0(term_count * sizeof(uint32 *));

	for (i = 0; i < term_count; i++)
	{
		char *lexeme_start = STRPTR(tsvector) + we[i].pos;
		int	  lexeme_len   = we[i].len;
		char *lexeme;

		/* Always allocate on heap for terms array */
		lexeme = palloc(lexeme_len + 1);
		memcpy(lexeme, lexeme_start, lexeme_len);
		lexeme[lexeme_len] = '\0';

		terms[i] = lexeme;

		/* Get frequency from TSVector - count positions or default to 1 */
		if (we[i].haspos)
		{
			int32 freq = (int32)POSDATALEN(tsvector, &we[i]);

			frequencies[i] = freq;

			if (positions != NULL && freq > 0)
			{
				WordEntryPos *posdata = POSDATAPTR(tsvector, &we[i]);
				int			  j;

				positions[i] = palloc(freq * sizeof(uint32));
				for (j = 0; j < freq; j++)
					positions[i][j] = (uint32)WEP_GETPOS(posdata[j]);
			}
		}
		else
		{
			frequencies[i] = 1;
			/* No position data — positions[i] stays NULL */
		}

		doc_length += frequencies[i];
	}

	*terms_out		 = terms;
	*frequencies_out = frequencies;
	if (positions_out)
		*positions_out = positions;

	return doc_length;
}

void
tp_free_term_positions(uint32 **positions, int term_count)
{
	int i;

	if (positions == NULL)
		return;
	for (i = 0; i < term_count; i++)
	{
		if (positions[i] != NULL)
			pfree(positions[i]);
	}
	pfree(positions);
}

/*
 * Free memory allocated for terms array
 */
static void
tp_free_terms_array(char **terms, int term_count)
{
	int i;

	if (terms == NULL)
		return;

	for (i = 0; i < term_count; i++)
		pfree(terms[i]);

	pfree(terms);
}

/*
 * Core document processing: convert text to terms and add to posting lists
 * This is shared between index building and docid recovery.
 *
 * If index_rel is provided, auto-spill will occur when memory limit is
 * exceeded. If index_rel is NULL, no auto-spill occurs (recovery path).
 */
bool
tp_process_document_text(
		text			  *document_text,
		ItemPointer		   ctid,
		Oid				   text_config_oid,
		TpLocalIndexState *index_state,
		Relation		   index_rel,
		int32			  *doc_length_out,
		uint8			   content_format)
{
	char	*document_str;
	Datum	 tsvector_datum;
	TSVector tsvector;
	char   **terms;
	int32	*frequencies;
	uint32 **positions = NULL;
	int		 term_count;
	int		 doc_length;

	if (!document_text || !index_state)
		return false;

	document_text = tp_normalize_markup(
			document_text, (TpContentFormat)content_format);
	document_str = text_to_cstring(document_text);

	/* Validate the TID before processing */
	if (!ItemPointerIsValid(ctid))
	{
		elog(WARNING,
			 "Invalid TID during document processing, skipping document");
		pfree(document_str);
		return false;
	}

	/* Vectorize the document using text configuration */
	tsvector_datum = DirectFunctionCall2Coll(
			to_tsvector_byid,
			InvalidOid, /* collation */
			ObjectIdGetDatum(text_config_oid),
			PointerGetDatum(document_text));

	tsvector = DatumGetTSVector(tsvector_datum);

	/* Extract lexemes, frequencies, and positions from TSVector */
	doc_length = tp_extract_terms_from_tsvector(
			tsvector, &terms, &frequencies, &positions, &term_count);

	if (term_count > 0)
	{
		/*
		 * Acquire exclusive lock for this transaction if not already held.
		 * During index build, we acquire once and hold for the entire build.
		 */
		tp_acquire_index_lock(index_state, LW_EXCLUSIVE);

		/* Add document terms to posting lists (single-col legacy caller) */
		tp_add_document_terms(
				index_state,
				ctid,
				terms,
				frequencies,
				positions,
				term_count,
				doc_length,
				NULL,
				0);

		/*
		 * Check memory after document completion and auto-spill if needed.
		 * Only spill if index_rel is provided (not during recovery).
		 */
		if (index_rel != NULL)
			tp_auto_spill_if_needed(index_state, index_rel);

		/* Free the terms array and individual lexemes */
		tp_free_terms_array(terms, term_count);
		pfree(frequencies);
		tp_free_term_positions(positions, term_count);
	}

	if (doc_length_out)
		*doc_length_out = doc_length;

	pfree(document_str);
	return true;
}

/*
 * Callback state for table_index_build_scan during serial builds.
 */
typedef struct TpBuildCallbackState
{
	TpBuildContext *build_ctx;
	Relation		index;
	Oid				text_config_oid;
	MemoryContext	per_doc_ctx;
	int				num_cols; /* >= 1; > 1 means multi-column */
	bool			is_text_array[TP_MAX_FIELDS]; /* per column */
	uint64			total_docs;
	uint64			total_len;
	/* Phase 6.1d: per-field token sums across all batches */
	uint64 total_len_per_field[TP_MAX_FIELDS];
	uint64 tuples_done;
	uint8  field_formats[TP_MAX_FIELDS];
} TpBuildCallbackState;

/*
 * Per-tuple callback for table_index_build_scan.
 *
 * Receives pre-evaluated index expression values from the scan,
 * tokenizes the document text, and adds it to the build context.
 */
static void
tp_build_callback(
		Relation	index,
		ItemPointer ctid,
		Datum	   *values,
		bool	   *isnull,
		bool		tupleIsAlive,
		void	   *state)
{
	TpBuildCallbackState *bs = (TpBuildCallbackState *)state;
	MemoryContext		  oldctx;
	bool				  multi_col					   = (bs->num_cols > 1);
	char				**all_terms					   = NULL;
	int32				 *all_freqs					   = NULL;
	uint32				**all_positions				   = NULL;
	int					  all_count					   = 0;
	int					  all_cap					   = 0;
	int					  total_doc_length			   = 0;
	int32				  field_lengths[TP_MAX_FIELDS] = {0};
	int					  col;

	/* Suppress unused parameter warnings for callback signature */
	(void)index;
	(void)tupleIsAlive;

	if (!ItemPointerIsValid(ctid))
		return;

	/* Skip tuple if ALL indexed columns are NULL */
	{
		bool any_non_null = false;
		for (col = 0; col < bs->num_cols; col++)
			if (!isnull[col])
			{
				any_non_null = true;
				break;
			}
		if (!any_non_null)
			return;
	}

	oldctx = MemoryContextSwitchTo(bs->per_doc_ctx);

	for (col = 0; col < bs->num_cols; col++)
	{
		text	*document_text;
		Datum	 tsvector_datum;
		TSVector tsvector;
		char   **terms;
		int32	*frequencies;
		uint32 **positions = NULL;
		int		 term_count;
		int		 doc_length;
		int		 i;

		if (isnull[col])
			continue;

		if (bs->is_text_array[col])
			document_text = tp_flatten_text_array(values[col]);
		else
			document_text = DatumGetTextPP(values[col]);

		document_text = tp_normalize_markup(
				document_text, (TpContentFormat)bs->field_formats[col]);

		tsvector_datum = DirectFunctionCall2Coll(
				to_tsvector_byid,
				InvalidOid,
				ObjectIdGetDatum(bs->text_config_oid),
				PointerGetDatum(document_text));
		tsvector = DatumGetTSVector(tsvector_datum);

		doc_length = tp_extract_terms_from_tsvector(
				tsvector, &terms, &frequencies, &positions, &term_count);

		/*
		 * Multi-col: prefix each term with a tag byte so dict entries
		 * for different columns stay distinct. Single-col indexes
		 * keep legacy untagged storage.
		 */
		if (multi_col && term_count > 0)
		{
			unsigned char tag = tp_field_tag_byte(col);
			for (i = 0; i < term_count; i++)
			{
				size_t oldlen = strlen(terms[i]);
				char  *tagged = palloc(oldlen + 2);
				tagged[0]	  = (char)tag;
				memcpy(tagged + 1, terms[i], oldlen + 1);
				pfree(terms[i]);
				terms[i] = tagged;
			}
		}

		/* Append this column's terms/freqs/positions to all_* arrays. */
		if (term_count > 0)
		{
			int need = all_count + term_count;
			if (need > all_cap)
			{
				int new_cap = (all_cap == 0) ? 16 : all_cap * 2;
				while (new_cap < need)
					new_cap *= 2;
				if (all_terms == NULL)
				{
					all_terms	  = palloc(new_cap * sizeof(char *));
					all_freqs	  = palloc(new_cap * sizeof(int32));
					all_positions = palloc(new_cap * sizeof(uint32 *));
				}
				else
				{
					all_terms = repalloc(all_terms, new_cap * sizeof(char *));
					all_freqs = repalloc(all_freqs, new_cap * sizeof(int32));
					all_positions = repalloc(
							all_positions, new_cap * sizeof(uint32 *));
				}
				all_cap = new_cap;
			}
			for (i = 0; i < term_count; i++)
			{
				all_terms[all_count]	 = terms[i];
				all_freqs[all_count]	 = frequencies[i];
				all_positions[all_count] = positions ? positions[i] : NULL;
				all_count++;
			}
			if (terms)
				pfree(terms);
			if (frequencies)
				pfree(frequencies);
			if (positions)
				pfree(positions);
		}

		total_doc_length += doc_length;
		if (col < TP_MAX_FIELDS)
			field_lengths[col] = doc_length;
	}

	MemoryContextSwitchTo(oldctx);

	if (all_count > 0)
	{
		if (multi_col)
			tp_build_context_add_document_multi(
					bs->build_ctx,
					all_terms,
					all_freqs,
					all_positions,
					all_count,
					total_doc_length,
					field_lengths,
					bs->num_cols,
					ctid);
		else
			tp_build_context_add_document(
					bs->build_ctx,
					all_terms,
					all_freqs,
					all_positions,
					all_count,
					total_doc_length,
					ctid);
	}

	/* Reset per-doc context (frees tsvector, terms) */
	MemoryContextReset(bs->per_doc_ctx);

	/* Budget-based flush */
	if (tp_build_context_should_flush(bs->build_ctx))
	{
		int f;

		bs->total_docs += bs->build_ctx->num_docs;
		bs->total_len += bs->build_ctx->total_len;
		for (f = 0; f < TP_MAX_FIELDS; f++)
			bs->total_len_per_field[f] +=
					bs->build_ctx->total_len_per_field[f];

		tp_build_flush_and_link(bs->build_ctx, bs->index);
		tp_build_context_reset(bs->build_ctx);

		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_COMPACTING);
		tp_maybe_compact_level(bs->index, 0);
		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_LOADING);
	}

	bs->tuples_done++;
	if (bs->tuples_done % TP_PROGRESS_REPORT_INTERVAL == 0)
	{
		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_TUPLES_DONE, bs->tuples_done);
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Build a new Tapir index
 */
IndexBuildResult *
tp_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult  *result;
	char			  *text_config_name = NULL;
	Oid				   text_config_oid	= InvalidOid;
	double			   k1, b;
	uint64			   total_docs = 0;
	uint64			   total_len  = 0;
	TpLocalIndexState *index_state;
	int				   num_cols;
	bool			   is_text_array[TP_MAX_FIELDS];
	char		field_name_buf[TP_MAX_FIELDS][TP_MAX_FIELD_NAME_LEN + 1];
	const char *field_names[TP_MAX_FIELDS];
	int			col;
	char	   *field_weights_str = NULL;
	float4		field_weights[TP_MAX_FIELDS];
	char	   *content_format_str = NULL;
	uint8		field_formats[TP_MAX_FIELDS];

	/* Show "started" for first partition only (suppresses duplicates) */
	if (!build_progress.active || build_progress.partition_count == 0)
		elog(NOTICE,
			 "BM25 index build started for relation %s",
			 RelationGetRelationName(index));

	/*
	 * Invalidate docid cache to prevent stale entries from a previous build.
	 * This is critical during VACUUM FULL, which creates a new index file
	 * with different block layout than the old one.
	 */
	tp_invalidate_docid_cache();

	/*
	 * Inspect each indexed column: capture its name (for the metapage
	 * field registry) and whether it's a text[] (for per-column
	 * flattening in tp_build_callback). Expression indexes
	 * (attnum == 0) get a synthetic "expr_<N>" name.
	 */
	num_cols = indexInfo->ii_NumIndexAttrs;
	if (num_cols < 1 || num_cols > TP_MAX_FIELDS)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_textsearch supports 1..%d indexed columns, got %d",
						TP_MAX_FIELDS,
						num_cols)));

	memset(is_text_array, 0, sizeof(is_text_array));
	memset(field_name_buf, 0, sizeof(field_name_buf));
	for (col = 0; col < num_cols; col++)
	{
		AttrNumber attnum = indexInfo->ii_IndexAttrNumbers[col];
		if (attnum > 0)
		{
			Form_pg_attribute att =
					TupleDescAttr(RelationGetDescr(heap), attnum - 1);
			is_text_array[col] = tp_is_text_array_type(att->atttypid);
			strlcpy(field_name_buf[col],
					NameStr(att->attname),
					TP_MAX_FIELD_NAME_LEN + 1);
		}
		else
		{
			is_text_array[col] = false;
			snprintf(
					field_name_buf[col],
					TP_MAX_FIELD_NAME_LEN + 1,
					"expr_%d",
					col);
		}
		field_names[col] = field_name_buf[col];
	}

	/* Report initialization phase */
	pgstat_progress_update_param(
			PROGRESS_CREATEIDX_SUBPHASE,
			PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE);

	/* Extract options from index */
	memset(field_weights, 0, sizeof(field_weights));
	memset(field_formats, 0, sizeof(field_formats));
	tp_build_extract_options(
			index,
			&text_config_name,
			&text_config_oid,
			&k1,
			&b,
			&field_weights_str,
			&content_format_str);

	if (field_weights_str != NULL)
		tp_parse_field_weights(field_weights_str, num_cols, field_weights);
	if (content_format_str != NULL)
		tp_parse_content_formats(
				content_format_str, num_cols, field_names, field_formats);

	/* Log configuration (only for first partition when active) */
	if (!build_progress.active || build_progress.partition_count == 0)
	{
		if (text_config_name)
			elog(NOTICE,
				 "Using text search configuration: %s",
				 text_config_name);
		elog(NOTICE, "Using index options: k1=%.2f, b=%.2f", k1, b);
	}

	/* Initialize metapage */
	tp_build_init_metapage(
			index,
			text_config_oid,
			k1,
			b,
			num_cols,
			field_names,
			field_weights_str != NULL ? field_weights : NULL,
			content_format_str != NULL ? field_formats : NULL);

	/*
	 * Check memory limits before starting build.
	 * The post-build transition allocates a runtime memtable
	 * in the global DSA, so try to free space first.
	 *
	 * Soft limit: try to evict if estimated usage is high.
	 * Hard limit: fail if DSA reservation exceeds the cap.
	 */
	if (tp_memory_limit > 0)
	{
		uint64 soft = tp_soft_limit_bytes();

		if (tp_get_estimated_total_bytes() > soft)
			tp_evict_largest_memtable(InvalidOid);

		{
			uint64 limit = tp_hard_limit_bytes();
			uint64 dsa_bytes;

			tp_registry_update_dsa_counter();
			dsa_bytes = tp_registry_get_total_dsa_bytes();

			if (dsa_bytes > limit)
			{
				tp_evict_largest_memtable(InvalidOid);

				/* Re-check after eviction attempt */
				tp_registry_update_dsa_counter();
				dsa_bytes = tp_registry_get_total_dsa_bytes();

				if (dsa_bytes > limit)
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("pg_textsearch DSA "
									"memory (" UINT64_FORMAT " kB) exceeds "
									"memory_limit "
									"(%d kB), cannot "
									"start index build",
									(uint64)(dsa_bytes / 1024),
									tp_memory_limit),
							 errhint("Increase "
									 "pg_textsearch."
									 "memory_limit or "
									 "spill indexes with "
									 "bm25_spill_index().")));
			}
		}
	}

	/*
	 * Check if parallel build is possible and beneficial.
	 *
	 * Postgres has already called plan_create_index_workers() and stored
	 * the result in indexInfo->ii_ParallelWorkers. We use that value
	 * directly to avoid redundant planning work and ensure consistency.
	 *
	 * We add our own minimum tuple threshold (100K) because for smaller
	 * tables, the parallel coordination overhead exceeds the benefit.
	 */
	{
		int	   nworkers	 = indexInfo->ii_ParallelWorkers;
		double reltuples = heap->rd_rel->reltuples;

		/*
		 * Only consider parallel build for tables with 100K+ estimated rows.
		 * For smaller tables, the parallel coordination overhead exceeds
		 * the benefit.
		 *
		 * If reltuples is -1 (table never analyzed), estimate from page count.
		 * We use a conservative estimate of 50 tuples per 8KB page, which
		 * assumes ~160 bytes per row (reasonable for text search workloads).
		 */
#define TP_MIN_PARALLEL_TUPLES		100000
#define TP_TUPLES_PER_PAGE_ESTIMATE 50

		if (reltuples < 0)
		{
			BlockNumber nblocks = RelationGetNumberOfBlocks(heap);
			reltuples = (double)nblocks * TP_TUPLES_PER_PAGE_ESTIMATE;
		}

		/*
		 * Thresholds for warning about suboptimal parallelism.
		 * These are conservative - we only warn when users could see
		 * significant (>2x) speedup from more parallelism.
		 */
#define TP_WARN_NO_PARALLEL_TUPLES 1000000 /* 1M tuples */
#define TP_WARN_FEW_WORKERS_TUPLES 5000000 /* 5M tuples */
#define TP_WARN_FEW_WORKERS_MIN	   2	   /* suggest more if <= this */

		if (nworkers > 0 && reltuples >= TP_MIN_PARALLEL_TUPLES &&
			!indexInfo->ii_Concurrent)
		{
			IndexBuildResult *par_result;

			/*
			 * Warn if table is very large but parallelism is limited.
			 * Suppress during partitioned builds to reduce noise.
			 */
			if (!build_progress.active &&
				reltuples >= TP_WARN_FEW_WORKERS_TUPLES &&
				nworkers <= TP_WARN_FEW_WORKERS_MIN)
			{
				elog(NOTICE,
					 "Large table (%.0f tuples) with only %d parallel "
					 "workers. "
					 "Consider increasing "
					 "max_parallel_maintenance_workers "
					 "and "
					 "maintenance_work_mem (need 32MB per worker) "
					 "for faster builds.",
					 reltuples,
					 nworkers);
			}

			/*
			 * Multi-column indexes are not yet supported by the
			 * parallel build path (worker callback + buffile writer
			 * remain single-column). Fall through to the serial
			 * build when num_cols > 1.
			 */
			if (num_cols > 1)
				goto skip_parallel_build;

			par_result = tp_build_parallel(
					heap,
					index,
					indexInfo,
					text_config_oid,
					k1,
					b,
					is_text_array[0],
					nworkers,
					field_formats);

			/*
			 * Create shared index state for runtime queries.
			 *
			 * The parallel build writes segments and updates
			 * the metapage, but does not create the in-memory
			 * shared state that INSERT and SELECT need.
			 * Without this, the first post-build access falls
			 * through to tp_rebuild_index_from_disk() (the
			 * crash-recovery path), which is fragile: a
			 * concurrent backend can race to recreate the
			 * state, leaving the inserting backend's memtable
			 * invisible to scans.
			 *
			 * By creating the state here — the same backend
			 * that ran the build — we ensure the registry
			 * entry and local cache are ready before the
			 * CREATE INDEX transaction commits.
			 */
			{
				TpLocalIndexState *pstate;
				Buffer			   metabuf;
				Page			   mpage;
				TpIndexMetaPage	   metap;

				pstate = tp_create_shared_index_state(
						RelationGetRelid(index), RelationGetRelid(heap));

				metabuf = ReadBuffer(index, TP_METAPAGE_BLKNO);
				LockBuffer(metabuf, BUFFER_LOCK_SHARE);
				mpage = BufferGetPage(metabuf);
				metap = (TpIndexMetaPage)PageGetContents(mpage);

				pg_atomic_write_u32(
						&pstate->shared->total_docs, metap->total_docs);
				pg_atomic_write_u64(
						&pstate->shared->total_len, metap->total_len);

				if (build_progress.active)
				{
					build_progress.total_docs += (uint64)metap->total_docs;
					build_progress.total_len += (uint64)metap->total_len;
					build_progress.partition_count++;
				}

				UnlockReleaseBuffer(metabuf);
			}

			return par_result;
		}

		if (!build_progress.active &&
			reltuples >= TP_WARN_NO_PARALLEL_TUPLES && nworkers == 0)
		{
			/*
			 * Large table but no parallel workers available.
			 * This is likely due to
			 * max_parallel_maintenance_workers = 0.
			 */
			elog(NOTICE,
				 "Large table (%.0f tuples) but parallel build "
				 "disabled. "
				 "Set max_parallel_maintenance_workers > 0 and "
				 "ensure "
				 "maintenance_work_mem >= 64MB for faster builds.",
				 reltuples);
		}
	}

skip_parallel_build:
	/*
	 * Serial build using arena-based build context.
	 *
	 * Uses table_index_build_scan() which automatically evaluates
	 * index expressions and partial-index predicates, passing
	 * pre-computed values to our callback.
	 * maintenance_work_mem controls the per-batch memory budget.
	 */
	{
		TpBuildCallbackState bs;
		TpBuildContext		*build_ctx;
		Size				 budget;
		double				 reltuples;

		/*
		 * Still create build index state for:
		 * - Per-index LWLock infrastructure
		 * - Post-build transition to runtime mode
		 * - Shared state initialization for runtime queries
		 */
		index_state = tp_create_build_index_state(
				RelationGetRelid(index), RelationGetRelid(heap));

		/* Budget: maintenance_work_mem (in KB) -> bytes */
		budget	  = (Size)maintenance_work_mem * 1024L;
		build_ctx = tp_build_context_create(budget);

		/*
		 * Phase 6.1d: enable per-field fieldnorm tracking when this
		 * index has > 1 columns.  Single-col goes through the legacy
		 * (total-length) path.
		 */
		if (num_cols > 1)
			tp_build_context_set_num_fields(build_ctx, num_cols);

		/* Initialize callback state */
		bs.build_ctx	   = build_ctx;
		bs.index		   = index;
		bs.text_config_oid = text_config_oid;
		bs.num_cols		   = num_cols;
		memcpy(bs.is_text_array, is_text_array, sizeof(bs.is_text_array));
		bs.per_doc_ctx = AllocSetContextCreate(
				CurrentMemoryContext,
				"build per-doc temp",
				ALLOCSET_DEFAULT_SIZES);
		bs.total_docs = 0;
		bs.total_len  = 0;
		memset(bs.total_len_per_field, 0, sizeof(bs.total_len_per_field));
		memcpy(bs.field_formats, field_formats, sizeof(bs.field_formats));
		bs.tuples_done = 0;

		/* Report loading phase */
		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_LOADING);

		{
			double rt		  = heap->rd_rel->reltuples;
			int64  tuples_est = (rt > 0) ? (int64)rt : 0;

			pgstat_progress_update_param(
					PROGRESS_CREATEIDX_TUPLES_TOTAL, tuples_est);
		}

		/* Scan table with expression evaluation */
		reltuples = table_index_build_scan(
				heap,
				index,
				indexInfo,
				true,
				false,
				tp_build_callback,
				&bs,
				NULL);

		/* Accumulate final batch stats */
		total_docs = bs.total_docs + build_ctx->num_docs;
		total_len  = bs.total_len + build_ctx->total_len;
		{
			int f;
			for (f = 0; f < TP_MAX_FIELDS; f++)
				bs.total_len_per_field[f] += build_ctx->total_len_per_field[f];
		}

		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_TUPLES_DONE, bs.tuples_done);

		/* Report writing phase */
		pgstat_progress_update_param(
				PROGRESS_CREATEIDX_SUBPHASE, TP_PHASE_WRITING);

		/* Write final segment if data remains */
		if (build_ctx->num_docs > 0)
			tp_build_flush_and_link(build_ctx, index);

		/* Update metapage with corpus statistics */
		{
			Buffer			  metabuf;
			GenericXLogState *state;
			Page			  metapage;
			TpIndexMetaPage	  metap;

			metabuf = ReadBuffer(index, TP_METAPAGE_BLKNO);
			LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

			state	 = GenericXLogStart(index);
			metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
			metap	 = (TpIndexMetaPage)PageGetContents(metapage);

			metap->total_docs = total_docs;
			metap->total_len  = total_len;
			memcpy(metap->total_len_per_field,
				   bs.total_len_per_field,
				   sizeof(metap->total_len_per_field));

			GenericXLogFinish(state);
			UnlockReleaseBuffer(metabuf);
		}

		/* Update shared state for runtime queries */
		pg_atomic_write_u32(&index_state->shared->total_docs, total_docs);
		pg_atomic_write_u64(&index_state->shared->total_len, total_len);

		/* Create index build result */
		result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
		result->heap_tuples	 = reltuples;
		result->index_tuples = total_docs;

		if (build_progress.active)
		{
			/* Accumulate stats for aggregated summary */
			build_progress.total_docs += total_docs;
			build_progress.total_len += total_len;
			build_progress.partition_count++;
		}
		else
		{
			elog(NOTICE,
				 "BM25 index build completed: " UINT64_FORMAT
				 " documents, avg_length=%.2f",
				 total_docs,
				 total_docs > 0 ? (float4)(total_len / (double)total_docs)
								: 0.0);
		}

		/*
		 * Release the per-index lock before finalizing.
		 * Critical for partitioned tables to avoid hitting
		 * MAX_SIMUL_LWLOCKS limit.
		 */
		tp_release_index_lock(index_state);

		/*
		 * Finalize build mode: destroy private DSA and
		 * transition to global DSA for runtime operation.
		 */
		tp_finalize_build_mode(index_state);

		/* Cleanup */
		tp_build_context_destroy(build_ctx);
		MemoryContextDelete(bs.per_doc_ctx);
	}

	return result;
}

/*
 * Build an empty Tapir index (for CREATE INDEX without data)
 */
void
tp_buildempty(Relation index)
{
	TpOptions	   *options;
	Buffer			metabuf;
	Page			metapage;
	TpIndexMetaPage metap;
	char		   *text_config_name = NULL;
	Oid				text_config_oid	 = InvalidOid;

	/* Extract options from index */
	options = (TpOptions *)index->rd_options;
	if (options)
	{
		if (options->text_config_offset > 0)
		{
			text_config_name = pstrdup(
					(char *)options + options->text_config_offset);
			{
				List *names =
						stringToQualifiedNameList(text_config_name, NULL);

				text_config_oid = get_ts_config_oid(names, false);
				list_free(names);
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("text_config parameter is required for bm25 "
							"indexes"),
					 errhint("Specify text_config when creating the index: "
							 "CREATE INDEX ... USING "
							 "bm25(column) WITH (text_config='english')")));
		}
	}
	else
	{
		/* No options provided - require text_config */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("text_config parameter is required for bm25 indexes"),
				 errhint("Specify text_config when creating the index: "
						 "CREATE INDEX ... USING "
						 "bm25(column) WITH (text_config='english')")));
	}

	/* Create and initialize the metapage */
	metabuf = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	Assert(BufferGetBlockNumber(metabuf) == TP_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	{
		GenericXLogState *state;

		state	 = GenericXLogStart(index);
		metapage = GenericXLogRegisterBuffer(
				state, metabuf, GENERIC_XLOG_FULL_IMAGE);

		tp_init_metapage(metapage, text_config_oid);

		/* Set additional parameters after init */
		metap	  = (TpIndexMetaPage)PageGetContents(metapage);
		metap->k1 = TP_DEFAULT_K1;
		metap->b  = TP_DEFAULT_B;

		GenericXLogFinish(state);
	}

	UnlockReleaseBuffer(metabuf);
}

/*
 * Insert a tuple into the Tapir index.
 *
 * Tokenization happens before any lock is acquired so that
 * CPU-intensive text processing does not serialize inserts.
 * The per-index lock is held as LW_SHARED for the memtable
 * write and docid-page update, then released before the
 * auto-spill check (which may need LW_EXCLUSIVE).
 */
bool
tp_insert(
		Relation		 index,
		Datum			*values,
		bool			*isnull,
		ItemPointer		 ht_ctid,
		Relation		 heapRel,
		IndexUniqueCheck checkUnique,
		bool			 indexUnchanged,
		IndexInfo		*indexInfo)
{
	int32			  *frequencies = NULL;
	uint32			 **positions   = NULL;
	int				   term_count  = 0;
	int				   doc_length  = 0;
	TpLocalIndexState *index_state;
	char			 **terms		   = NULL;
	Oid				   text_config_oid = InvalidOid;
	int				   num_cols;
	int				   num_fields_in_metap;
	bool			   multi_col;
	int				   col;
	int				   all_cap						= 0;
	int32			   field_lengths[TP_MAX_FIELDS] = {0};

	(void)checkUnique;	  /* unused */
	(void)indexUnchanged; /* unused */

	num_cols = indexInfo->ii_NumIndexAttrs;
	if (num_cols < 1 || num_cols > TP_MAX_FIELDS)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_textsearch supports 1..%d indexed columns, got %d",
						TP_MAX_FIELDS,
						num_cols)));

	/*
	 * Look up the index's text_config_oid AND multi-col field count
	 * from the metapage. multi_col is true iff the metapage registry
	 * has ≥ 2 fields — independent of num_cols in indexInfo, which
	 * should match in practice but we trust the metapage.
	 */
	uint8 field_formats_ins[TP_MAX_FIELDS];
	memset(field_formats_ins, 0, sizeof(field_formats_ins));
	{
		TpIndexMetaPage metap = tp_get_metapage(index);
		text_config_oid		  = metap->text_config_oid;
		num_fields_in_metap	  = metap->num_fields;
		memcpy(field_formats_ins,
			   metap->field_formats,
			   sizeof(field_formats_ins));
		pfree(metap);
	}
	multi_col = num_fields_in_metap > 1;

	/* Skip tuple if all indexed columns are NULL */
	{
		bool any_non_null = false;
		for (col = 0; col < num_cols; col++)
			if (!isnull[col])
			{
				any_non_null = true;
				break;
			}
		if (!any_non_null)
			return true;
	}

	/* --- Phase 1: Tokenize per column (no lock held) --- */
	for (col = 0; col < num_cols; col++)
	{
		text	*document_text;
		Datum	 tsvector_datum;
		TSVector tsvector;
		char   **col_terms;
		int32	*col_frequencies;
		uint32 **col_positions = NULL;
		int		 col_term_count;
		int		 col_doc_length;
		int		 i;
		bool	 is_text_array_col;

		if (isnull[col])
			continue;

		{
			AttrNumber attnum = indexInfo->ii_IndexAttrNumbers[col];
			if (attnum > 0)
			{
				Oid atttype =
						TupleDescAttr(RelationGetDescr(heapRel), attnum - 1)
								->atttypid;
				is_text_array_col = tp_is_text_array_type(atttype);
			}
			else
				is_text_array_col = false;
		}

		if (is_text_array_col)
			document_text = tp_flatten_text_array(values[col]);
		else
			document_text = DatumGetTextPP(values[col]);

		document_text = tp_normalize_markup(
				document_text, (TpContentFormat)field_formats_ins[col]);

		if (OidIsValid(text_config_oid))
		{
			tsvector_datum = DirectFunctionCall2(
					to_tsvector_byid,
					ObjectIdGetDatum(text_config_oid),
					PointerGetDatum(document_text));
		}
		else
		{
			tsvector_datum = DirectFunctionCall1(
					to_tsvector, PointerGetDatum(document_text));
		}
		tsvector = DatumGetTSVector(tsvector_datum);

		col_doc_length = tp_extract_terms_from_tsvector(
				tsvector,
				&col_terms,
				&col_frequencies,
				&col_positions,
				&col_term_count);

		if (multi_col && col_term_count > 0)
		{
			unsigned char tag = tp_field_tag_byte(col);
			for (i = 0; i < col_term_count; i++)
			{
				size_t oldlen = strlen(col_terms[i]);
				char  *tagged = palloc(oldlen + 2);
				tagged[0]	  = (char)tag;
				memcpy(tagged + 1, col_terms[i], oldlen + 1);
				pfree(col_terms[i]);
				col_terms[i] = tagged;
			}
		}

		if (col_term_count > 0)
		{
			int need = term_count + col_term_count;
			if (need > all_cap)
			{
				int new_cap = (all_cap == 0) ? 16 : all_cap * 2;
				while (new_cap < need)
					new_cap *= 2;
				if (terms == NULL)
				{
					terms		= palloc(new_cap * sizeof(char *));
					frequencies = palloc(new_cap * sizeof(int32));
					positions	= palloc(new_cap * sizeof(uint32 *));
				}
				else
				{
					terms = repalloc(terms, new_cap * sizeof(char *));
					frequencies =
							repalloc(frequencies, new_cap * sizeof(int32));
					positions =
							repalloc(positions, new_cap * sizeof(uint32 *));
				}
				all_cap = new_cap;
			}
			for (i = 0; i < col_term_count; i++)
			{
				terms[term_count]		= col_terms[i];
				frequencies[term_count] = col_frequencies[i];
				positions[term_count]	= col_positions ? col_positions[i]
														: NULL;
				term_count++;
			}
			if (col_terms)
				pfree(col_terms);
			if (col_frequencies)
				pfree(col_frequencies);
			if (col_positions)
				pfree(col_positions);
		}

		doc_length += col_doc_length;
		if (col < TP_MAX_FIELDS)
			field_lengths[col] = col_doc_length;
	}

	/* --- Phase 2: Shared-memory work (under lock) --- */
	index_state = tp_get_local_index_state(RelationGetRelid(index));

	if (index_state != NULL && term_count > 0)
	{
		/*
		 * Hard limit check before acquiring the per-index lock.
		 * This is a simple atomic read of the global DSA counter
		 * — it does not flush or evict any memtable.  If over
		 * the hard limit, we ERROR out before touching any
		 * shared state.
		 */
		tp_check_hard_limit();

		/*
		 * Acquire per-index lock. Normally LW_SHARED so
		 * multiple inserters run concurrently.
		 *
		 * Cold path: if the memtable hash tables are not
		 * yet initialized, acquire LW_EXCLUSIVE and init.
		 * We stay exclusive for the cold-path insert to
		 * prevent a concurrent spill from clearing the
		 * tables between init and use.
		 *
		 * The lockless pre-check is an optimization to
		 * avoid exclusive on the hot path. If a concurrent
		 * spill invalidates between the check and lock
		 * acquire, we detect it under the lock and retry.
		 */
		for (;;)
		{
			TpMemtable *mt		  = get_memtable(index_state);
			bool		need_init = mt &&
							 (mt->string_hash_handle ==
									  DSHASH_HANDLE_INVALID ||
							  mt->doc_lengths_handle == DSHASH_HANDLE_INVALID);

			tp_acquire_index_lock(
					index_state, need_init ? LW_EXCLUSIVE : LW_SHARED);

			if (need_init)
			{
				tp_ensure_string_table_initialized(index_state);
				break; /* Hold exclusive for insert */
			}

			/*
			 * Re-check under shared lock: a spill may
			 * have cleared the tables after our lockless
			 * read but before we acquired shared.
			 */
			mt = get_memtable(index_state);
			if (mt && (mt->string_hash_handle == DSHASH_HANDLE_INVALID ||
					   mt->doc_lengths_handle == DSHASH_HANDLE_INVALID))
			{
				/* Stale read — retry with exclusive */
				tp_release_index_lock(index_state);
				continue;
			}
			break;
		}

		/* Validate TID before adding to posting list */
		if (!ItemPointerIsValid(ht_ctid))
			elog(WARNING, "Invalid TID in tp_insert, skipping");
		else
		{
			tp_add_document_terms(
					index_state,
					ht_ctid,
					terms,
					frequencies,
					positions,
					term_count,
					doc_length,
					multi_col ? field_lengths : NULL,
					multi_col ? num_cols : 0);

			/*
			 * Docid pages under LW_SHARED — spill clears
			 * these under LW_EXCLUSIVE, so they must be
			 * written while we hold shared.
			 */
			tp_add_docid_to_pages(index, ht_ctid);
		}

		/* Release lock before spill check */
		tp_release_index_lock(index_state);

		/*
		 * Auto-spill check runs outside the lock — may
		 * acquire LW_EXCLUSIVE if a spill is needed.
		 */
		tp_auto_spill_if_needed(index_state, index);
	}
	else if (term_count > 0 && ItemPointerIsValid(ht_ctid))
	{
		/* No shared state but valid doc — record TID */
		tp_add_docid_to_pages(index, ht_ctid);
	}

	/* Free the terms array and individual lexemes */
	if (terms)
	{
		int t;
		for (t = 0; t < term_count; t++)
			pfree(terms[t]);
		pfree(terms);
		pfree(frequencies);
		tp_free_term_positions(positions, term_count);
	}

	return true;
}
