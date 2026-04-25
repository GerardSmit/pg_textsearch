/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * scan.c - BM25 index scan operations
 */
#include <postgres.h>

#include <access/genam.h>
#include <access/parallel.h>
#include <access/relscan.h>
#include <access/sdir.h>
#include <access/table.h>
#include <catalog/namespace.h>
#include <pgstat.h>
#include <storage/bufmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/regproc.h>
#include <utils/rel.h>

#include "access/am.h"
#include "access/scan_parallel.h"
#include "constants.h"
#include "index/limit.h"
#include "index/metapage.h"
#include "index/resolve.h"
#include "index/state.h"
#include "memtable/scan.h"
#include "scoring/bm25.h"
#include "scoring/bmw.h"
#include "types/query.h"
#include "types/query_parser.h"
#include "types/vector.h"

/*
 * Backend-local cached score for ORDER BY optimization.
 *
 * When tp_gettuple returns a row, the BM25 score is cached here. The
 * bm25_get_current_score() stub function returns this value, avoiding
 * re-computation of scores in resjunk ORDER BY expressions.
 */
static float8 tp_cached_score = 0.0;

float8
tp_get_cached_score(void)
{
	return tp_cached_score;
}

/*
 * Clean up any previous scan results in the scan opaque structure
 */
static void
tp_rescan_cleanup_results(TpScanOpaque so)
{
	if (!so)
		return;

	Assert(so->scan_context != NULL);

	/* Clean up result CTIDs */
	if (so->result_ctids)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(so->scan_context);
		pfree(so->result_ctids);
		so->result_ctids = NULL;
		MemoryContextSwitchTo(oldcontext);
	}

	/* Clean up result scores */
	if (so->result_scores)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(so->scan_context);
		pfree(so->result_scores);
		so->result_scores = NULL;
		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * Process ORDER BY scan keys for <@> operator
 *
 * Handles both bm25query and plain text arguments to support:
 * - ORDER BY content <@> 'query'::bm25query (explicit bm25query)
 * - ORDER BY content <@> 'query' (plain text, implicit index resolution)
 */
static void
tp_rescan_process_orderby(
		IndexScanDesc	scan,
		ScanKey			orderbys,
		int				norderbys,
		TpIndexMetaPage metap)
{
	TpScanOpaque so = (TpScanOpaque)scan->opaque;

	for (int i = 0; i < norderbys; i++)
	{
		ScanKey orderby = &orderbys[i];

		/* Check for <@> operator strategy */
		if (orderby->sk_strategy == 1) /* Strategy 1: <@> operator */
		{
			Datum query_datum = orderby->sk_argument;
			char *query_cstr;
			Oid	  query_index_oid		   = InvalidOid;
			bool  query_grammar			   = false;
			bool  query_fuzzy			   = false;
			uint8 query_fuzzy_max_distance = 0;

			/*
			 * Use sk_subtype to determine the argument type.
			 * sk_subtype contains the right-hand operand's type OID.
			 */
			if (orderby->sk_subtype == TEXTOID)
			{
				/* Plain text - use text directly */
				text *query_text = (text *)DatumGetPointer(query_datum);

				query_cstr = text_to_cstring(query_text);
			}
			else
			{
				/* bm25query - extract query text and index OID */
				TpQuery *query = (TpQuery *)DatumGetPointer(query_datum);

				query_cstr				 = pstrdup(get_tpquery_text(query));
				query_index_oid			 = get_tpquery_index_oid(query);
				query_grammar			 = tpquery_is_grammar(query);
				query_fuzzy				 = tpquery_is_fuzzy(query);
				query_fuzzy_max_distance = tpquery_fuzzy_max_distance(query);

				/* Validate index OID if provided in query */
				if (tpquery_has_index(query))
				{
					tp_validate_query_index(
							query_index_oid, scan->indexRelation);
				}
			}

			/* Clear query vector since we're using text directly */
			if (so->query_vector)
			{
				pfree(so->query_vector);
				so->query_vector = NULL;
			}

			/* Free old query text if it exists */
			if (so->query_text)
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(
						so->scan_context);
				pfree(so->query_text);
				MemoryContextSwitchTo(oldcontext);
			}

			/* Allocate new query text in scan context */
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(
						so->scan_context);
				so->query_text = pstrdup(query_cstr);
				MemoryContextSwitchTo(oldcontext);
			}

			/* Store index OID for this scan */
			so->index_oid	  = RelationGetRelid(scan->indexRelation);
			so->query_grammar = query_grammar;
			so->query_fuzzy	  = query_fuzzy;
			so->query_fuzzy_max_distance = query_fuzzy_max_distance;

			/* Mark all docs as candidates for ORDER BY operation */
			if (metap && metap->total_docs > 0)
				so->result_count = metap->total_docs;

			pfree(query_cstr);
		}
	}
}

/*
 * Parallel-scan callbacks (DSM sizing + initialization).
 */
Size
tp_parallel_scan_size(void)
{
	return MAXALIGN(sizeof(TpParallelScan));
}

Size
#if PG_VERSION_NUM >= 180000
tp_estimateparallelscan(Relation rel, int nkeys, int norderbys)
{
	(void)rel;
#else
tp_estimateparallelscan(int nkeys, int norderbys)
{
#endif
	(void)nkeys;
	(void)norderbys;
	return tp_parallel_scan_size();
}

void
tp_initparallelscan(void *target)
{
	TpParallelScan *pscan = (TpParallelScan *)target;

	pscan->num_segments = 0;
	memset(pscan->segment_roots, 0, sizeof(pscan->segment_roots));
	pg_atomic_init_u32(&pscan->setup_done, 0);
	pg_atomic_init_u32(&pscan->next_segment_index, 0);
	pg_atomic_init_u32(&pscan->workers_attached, 0);
	pg_atomic_init_u32(&pscan->workers_done, 0);
}

void
tp_parallelrescan(IndexScanDesc scan)
{
	if (scan == NULL || scan->parallel_scan == NULL)
		return;

	{
#if PG_VERSION_NUM >= 180000
		TpParallelScan *pscan = (TpParallelScan *)OffsetToPointer(
				(void *)scan->parallel_scan,
				scan->parallel_scan->ps_offset_am);
#else
		TpParallelScan *pscan = (TpParallelScan *)OffsetToPointer(
				(void *)scan->parallel_scan, scan->parallel_scan->ps_offset);
#endif

		pg_atomic_write_u32(&pscan->setup_done, 0);
		pg_atomic_write_u32(&pscan->next_segment_index, 0);
		pg_atomic_write_u32(&pscan->workers_attached, 0);
		pg_atomic_write_u32(&pscan->workers_done, 0);
	}
}

IndexScanDesc
tp_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	TpScanOpaque  so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/* Allocate and initialize scan opaque data */
	so				 = (TpScanOpaque)palloc0(sizeof(TpScanOpaqueData));
	so->scan_context = AllocSetContextCreate(
			CurrentMemoryContext,
			"Tapir Scan Context",
			ALLOCSET_DEFAULT_SIZES);
	so->limit			 = -1; /* Initialize limit to -1 (no limit) */
	so->max_results_used = 0;
	scan->opaque		 = so;

	/*
	 * Custom index AMs must allocate ORDER BY arrays themselves.
	 */
	if (norderbys > 0)
	{
		scan->xs_orderbyvals  = (Datum *)palloc0(sizeof(Datum) * norderbys);
		scan->xs_orderbynulls = (bool *)palloc(sizeof(bool) * norderbys);
		/* Initialize all orderbynulls to true */
		memset(scan->xs_orderbynulls, true, sizeof(bool) * norderbys);
	}

	return scan;
}

/*
 * Restart a scan with new keys
 */
void
tp_rescan(
		IndexScanDesc scan,
		ScanKey		  keys __attribute__((unused)),
		int			  nkeys __attribute__((unused)),
		ScanKey		  orderbys,
		int			  norderbys)
{
	TpScanOpaque	so	  = (TpScanOpaque)scan->opaque;
	TpIndexMetaPage metap = NULL;

	Assert(scan != NULL);
	Assert(scan->opaque != NULL);

	if (!so)
		return;

	/* Retrieve query LIMIT, if available */
	{
		int query_limit = tp_get_query_limit(scan->indexRelation);
		so->limit		= (query_limit > 0) ? query_limit : -1;
	}

	/* Reset scan state */
	if (so)
	{
		/* Clean up any previous results */
		tp_rescan_cleanup_results(so);

		/* Reset scan position and state */
		so->current_pos	 = 0;
		so->result_count = 0;
		so->eof_reached	 = false;
		so->query_vector = NULL;
	}

	/* Process ORDER BY scan keys for <@> operator */
	if (norderbys > 0 && orderbys && so)
	{
		/* Get index metadata to check if we have documents */
		if (!metap)
			metap = tp_get_metapage(scan->indexRelation);

		tp_rescan_process_orderby(scan, orderbys, norderbys, metap);

		if (metap)
			pfree(metap);
	}
}

/*
 * End a scan and cleanup resources
 */
void
tp_endscan(IndexScanDesc scan)
{
	TpScanOpaque so = (TpScanOpaque)scan->opaque;

	if (so)
	{
		if (so->scan_context)
			MemoryContextDelete(so->scan_context);

		/* Free query vector if it was allocated */
		if (so->query_vector)
			pfree(so->query_vector);

		pfree(so);
		scan->opaque = NULL;
	}

	/*
	 * Don't free ORDER BY arrays here - PostgreSQL's core code will free them.
	 */
	if (scan->numberOfOrderBys > 0)
	{
		scan->xs_orderbyvals  = NULL;
		scan->xs_orderbynulls = NULL;
	}
}

/* ----------------------------------------------------------------
 * Parallel index scan: worker + leader paths
 * ----------------------------------------------------------------
 *
 * Leader populates TpParallelScan in DSM (segment roots, IDF, BM25
 * params), then scores memtable + claims segments.  Workers wait for
 * setup_done, claim segments via atomic counter, deposit results to
 * per-worker DSM slots, bump workers_done.  Leader merges after all
 * attached workers finish.
 */

static TpParallelScan *
tp_get_parallel_scan(IndexScanDesc scan)
{
	if (!scan->parallel_scan)
		return NULL;
#if PG_VERSION_NUM >= 180000
	return (TpParallelScan *)OffsetToPointer(
			(void *)scan->parallel_scan, scan->parallel_scan->ps_offset_am);
#else
	return (TpParallelScan *)OffsetToPointer(
			(void *)scan->parallel_scan, scan->parallel_scan->ps_offset);
#endif
}

/*
 * Claim segments from the work-stealing counter and score each
 * into the given heap using single-term BMW.
 */
static void
tp_parallel_claim_and_score(
		IndexScanDesc scan, TpParallelScan *pscan, TpTopKHeap *heap)
{
	for (;;)
	{
		uint32			 idx;
		TpSegmentReader *reader;

		idx = pg_atomic_fetch_add_u32(&pscan->next_segment_index, 1);
		if (idx >= (uint32)pscan->num_segments)
			break;

		reader = tp_segment_open(
				scan->indexRelation, pscan->segment_roots[idx]);
		score_segment_single_term_bmw(
				heap,
				reader,
				pscan->query_term,
				pscan->idf,
				pscan->k1,
				pscan->b,
				pscan->term_avgdl,
				NULL);
		tp_segment_close(reader);
	}
}

/*
 * Worker path: wait for leader setup, claim segments, deposit
 * results to DSM slot, signal done.  Always returns false (EOF).
 */
static bool
tp_parallel_worker_run(
		IndexScanDesc	   scan,
		TpScanOpaque	   so,
		TpLocalIndexState *index_state,
		TpParallelScan	  *pscan)
{
	int					  slot;
	uint32				  state;
	TpTopKHeap			  heap;
	TpParallelWorkerSlot *ws;
	int					  i;

	slot = ParallelWorkerNumber;
	if (slot < 0 || slot >= TP_PARALLEL_MAX_WORKERS)
	{
		so->eof_reached = true;
		return false;
	}

	pg_atomic_fetch_add_u32(&pscan->workers_attached, 1);

	/* Spin-wait for leader to finish setup */
	for (;;)
	{
		state = pg_atomic_read_u32(&pscan->setup_done);
		if (state != 0)
			break;
		CHECK_FOR_INTERRUPTS();
		pg_usleep(100);
	}

	/* Ineligible → return EOF */
	if (state == 2)
	{
		pg_atomic_fetch_add_u32(&pscan->workers_done, 1);
		so->eof_reached = true;
		return false;
	}

	/* Score claimed segments into a local heap */
	tp_topk_init(&heap, pscan->max_results, CurrentMemoryContext);
	tp_parallel_claim_and_score(scan, pscan, &heap);

	/* Deposit results to DSM slot */
	ws		  = &pscan->workers[slot];
	ws->count = heap.size;
	for (i = 0; i < heap.size; i++)
	{
		ws->entries[i].seg_block = heap.seg_blocks[i];
		ws->entries[i].doc_id	 = heap.doc_ids[i];
		ws->entries[i].score	 = heap.scores[i];
	}

	tp_topk_free(&heap);
	pg_atomic_fetch_add_u32(&pscan->workers_done, 1);
	so->eof_reached = true;
	return false;
}

/*
 * Leader path: check eligibility, set up DSM, score memtable +
 * segments, wait for workers, merge, extract results.
 *
 * Returns true if parallel path ran (results in so->result_ctids).
 * Returns false if query is ineligible (caller falls to serial).
 */
static bool
tp_parallel_leader_run(
		IndexScanDesc	   scan,
		TpScanOpaque	   so,
		TpLocalIndexState *index_state,
		TpParallelScan	  *pscan,
		TpIndexMetaPage	   metap)
{
	TpVector	  *query_vector;
	TpVectorEntry *entry;
	char		  *term;
	int32		   total_docs;
	float4		   avg_doc_len;
	float4		   field_weights[TP_MAX_FIELDS];
	float4		   field_avgdls[TP_MAX_FIELDS];
	BlockNumber	   level_heads[TP_MAX_LEVELS];
	uint32		   doc_freq;
	float4		   idf;
	float4		   term_avgdl;
	int			   max_results;
	int			   num_roots;
	BlockNumber	  *roots;
	TpTopKHeap	   heap;
	int			   result_count;
	int			   i;
	uint32		   attached;
	MemoryContext  oldctx;

	/* Grammar/fuzzy/multi-col → ineligible for parallel */
	if (so->query_text != NULL && (so->query_grammar || so->query_fuzzy ||
								   tp_metapage_is_multi_col(metap)))
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		return false;
	}

	/* Tokenize query */
	{
		char *idx_name = tp_get_qualified_index_name(scan->indexRelation);
		text *idx_text = cstring_to_text(idx_name);
		text *q_text   = cstring_to_text(so->query_text);
		Datum qv_datum = DirectFunctionCall2(
				to_tpvector,
				PointerGetDatum(q_text),
				PointerGetDatum(idx_text));

		query_vector = (TpVector *)DatumGetPointer(qv_datum);
	}

	if (!query_vector || query_vector->entry_count == 0)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		return false;
	}

	/* Only single-term queries for now */
	if (query_vector->entry_count != 1)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		return false;
	}

	/* Extract the single term */
	entry = get_tpvector_first_entry(query_vector);

	/* Issue #3: term too long for DSM fixed-size buffer */
	if (entry->lexeme_len >= NAMEDATALEN)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		return false;
	}

	term = palloc(entry->lexeme_len + 1);
	memcpy(term, entry->lexeme, entry->lexeme_len);
	term[entry->lexeme_len] = '\0';

	/* Read corpus stats */
	total_docs = pg_atomic_read_u32(&index_state->shared->total_docs);
	if (total_docs <= 0)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		pfree(term);
		return false;
	}
	avg_doc_len = (float4)(pg_atomic_read_u64(
								   &index_state->shared->total_len) /
						   (double)total_docs);
	if (avg_doc_len <= 0.0f)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		pfree(term);
		return false;
	}

	for (i = 0; i < TP_MAX_LEVELS; i++)
		level_heads[i] = metap->level_heads[i];
	memcpy(field_weights, metap->field_weights, sizeof(field_weights));
	for (i = 0; i < TP_MAX_FIELDS; i++)
		field_avgdls[i] = tp_metapage_field_avgdl(metap, i, avg_doc_len);

	/* Compute IDF */
	doc_freq = tp_get_unified_doc_freq(
			index_state, scan->indexRelation, term, level_heads);
	if (doc_freq == 0)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		pfree(term);
		return false;
	}
	idf = tp_calculate_idf(doc_freq, total_docs);
	idf *= tp_term_field_weight(term, field_weights);

	/* Per-field avgdl for tagged terms */
	term_avgdl = avg_doc_len;
	{
		unsigned char first = (unsigned char)term[0];
		if (first >= TP_FIELD_TAG_BASE)
		{
			int fidx = first - TP_FIELD_TAG_BASE;
			if (fidx >= 0 && fidx < TP_MAX_FIELDS && field_avgdls[fidx] > 0.0f)
				term_avgdl = field_avgdls[fidx];
		}
	}

	/* Collect segment roots */
	num_roots = tp_collect_segment_roots(scan->indexRelation, metap, &roots);

	/* Determine max_results */
	max_results = (so->limit > 0) ? so->limit : tp_default_limit;
	if (max_results > TP_PARALLEL_MAX_K ||
		num_roots > TP_PARALLEL_MAX_SEGMENTS)
	{
		pg_atomic_write_u32(&pscan->setup_done, 2);
		pfree(term);
		if (roots)
			pfree(roots);
		return false;
	}

	/* Populate shared DSM state — clear worker slots first */
	for (i = 0; i < TP_PARALLEL_MAX_WORKERS; i++)
		pscan->workers[i].count = 0;

	pscan->num_segments = num_roots;
	for (i = 0; i < num_roots; i++)
		pscan->segment_roots[i] = roots[i];
	pscan->max_results = max_results;
	memcpy(pscan->query_term, term, entry->lexeme_len + 1);
	pscan->idf		  = idf;
	pscan->term_avgdl = term_avgdl;
	pscan->k1		  = metap->k1;
	pscan->b		  = metap->b;

	/* Signal workers: setup done, eligible */
	pg_write_barrier();
	pg_atomic_write_u32(&pscan->setup_done, 1);

	/* ---- Leader scoring ---- */
	tp_topk_init(&heap, max_results, CurrentMemoryContext);

	/* Score memtable (leader-only) */
	score_memtable_single_term(
			&heap,
			index_state,
			term,
			idf,
			metap->k1,
			metap->b,
			term_avgdl,
			NULL);

	/* Leader also claims segments from work-stealing counter */
	tp_parallel_claim_and_score(scan, pscan, &heap);

	/* Wait for all attached workers to finish (no timeout —
	 * returning early would silently drop worker results) */
	pg_usleep(1000);
	for (;;)
	{
		attached = pg_atomic_read_u32(&pscan->workers_attached);
		if (pg_atomic_read_u32(&pscan->workers_done) >= attached)
			break;
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000);
	}

	/* Merge worker DSM slots into leader heap */
	for (i = 0; i < TP_PARALLEL_MAX_WORKERS; i++)
	{
		TpParallelWorkerSlot *ws = &pscan->workers[i];
		int					  j;

		if (ws->count <= 0)
			continue;
		for (j = 0; j < ws->count; j++)
		{
			if (!tp_topk_dominated(&heap, ws->entries[j].score))
			{
				tp_topk_add_segment(
						&heap,
						ws->entries[j].seg_block,
						ws->entries[j].doc_id,
						ws->entries[j].score);
			}
		}
	}

	/* Resolve CTIDs and extract */
	tp_topk_resolve_ctids(&heap, scan->indexRelation);

	oldctx			 = MemoryContextSwitchTo(so->scan_context);
	so->result_ctids = palloc(max_results * sizeof(ItemPointerData));
	memset(so->result_ctids, 0, max_results * sizeof(ItemPointerData));
	so->result_scores = palloc(max_results * sizeof(float4));

	result_count = tp_topk_extract(&heap, so->result_ctids, so->result_scores);
	MemoryContextSwitchTo(oldctx);

	so->result_count	 = result_count;
	so->current_pos		 = 0;
	so->max_results_used = max_results;

	tp_topk_free(&heap);
	pfree(term);
	if (roots)
		pfree(roots);

	return result_count > 0;
}

/*
 * Execute BM25 scoring query to get ordered results
 */
static bool
tp_execute_scoring_query(IndexScanDesc scan)
{
	TpScanOpaque	   so = (TpScanOpaque)scan->opaque;
	TpIndexMetaPage	   metap;
	bool			   success	   = false;
	TpLocalIndexState *index_state = NULL;
	TpVector		  *query_vector;

	if (!so || !so->query_text)
		return false;

	Assert(so->scan_context != NULL);

	/* Clean up previous results */
	if (so->result_ctids || so->result_scores)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(so->scan_context);

		if (so->result_ctids)
		{
			pfree(so->result_ctids);
			so->result_ctids = NULL;
		}
		if (so->result_scores)
		{
			pfree(so->result_scores);
			so->result_scores = NULL;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	so->result_count = 0;
	so->current_pos	 = 0;

	/* Get the index state with posting lists */
	index_state = tp_get_local_index_state(
			RelationGetRelid(scan->indexRelation));

	if (!index_state)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not get index state for BM25 "
						"search")));
	}

	/*
	 * Acquire shared lock BEFORE reading metapage.
	 * This ensures the metapage and memtable are read in a
	 * consistent state — spill (which rewrites both) requires
	 * LW_EXCLUSIVE, which is blocked while we hold shared.
	 */
	tp_acquire_index_lock(index_state, LW_SHARED);

	/* Now read metapage under the lock */
	metap = tp_get_metapage(scan->indexRelation);
	if (!metap)
	{
		tp_release_index_lock(index_state);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to get metapage for index %s",
						RelationGetRelationName(scan->indexRelation))));
	}

	/* Parallel scan dispatch */
	if (scan->parallel_scan != NULL)
	{
		TpParallelScan *pscan = tp_get_parallel_scan(scan);

		if (pscan)
		{
			if (IsParallelWorker())
			{
				tp_parallel_worker_run(scan, so, index_state, pscan);
				tp_release_index_lock(index_state);
				pfree(metap);
				return false;
			}

			/* Leader: try parallel path */
			if (tp_parallel_leader_run(scan, so, index_state, pscan, metap))
			{
				tp_release_index_lock(index_state);
				pfree(metap);
				return true;
			}

			/*
			 * Ineligible for parallel — setup_done already set
			 * to 2, workers will return EOF.  Fall through to
			 * serial path.
			 */
		}
	}

	/*
	 * Serial path.
	 *
	 * Route through the grammar-aware path when the query has a
	 * grammar extension OR when the index is multi-column (whose
	 * dictionary terms are all tag-prefixed, requiring the resolver
	 * to either expand unqualified queries across fields or apply
	 * field:term scoping).
	 */
	if (so->query_text != NULL && (so->query_grammar || so->query_fuzzy ||
								   tp_metapage_is_multi_col(metap)))
	{
		success = tp_memtable_search_with_grammar(
				scan,
				index_state,
				so->query_text,
				metap,
				so->query_fuzzy,
				so->query_fuzzy_max_distance);
		tp_release_index_lock(index_state);
		pfree(metap);
		return success;
	}

	/* Use the original query vector or create one from text */
	query_vector = so->query_vector;

	if (!query_vector && so->query_text)
	{
		/*
		 * We have a text query - convert it to a vector using the index.
		 */
		char *index_name = tp_get_qualified_index_name(scan->indexRelation);

		text *index_name_text  = cstring_to_text(index_name);
		text *query_text_datum = cstring_to_text(so->query_text);

		Datum query_vec_datum = DirectFunctionCall2(
				to_tpvector,
				PointerGetDatum(query_text_datum),
				PointerGetDatum(index_name_text));

		query_vector = (TpVector *)DatumGetPointer(query_vec_datum);

		/* Free existing query vector if present */
		if (so->query_vector)
			pfree(so->query_vector);

		/* Store the converted vector for this query execution */
		so->query_vector = query_vector;
	}

	if (!query_vector)
	{
		pfree(metap);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no query vector available in scan state")));
	}

	/* Find documents matching the query using posting lists */
	success = tp_memtable_search(scan, index_state, query_vector, metap);

	/* Release the lock - we've extracted all CTIDs we need */
	tp_release_index_lock(index_state);

	pfree(metap);
	return success;
}

/*
 * Get next tuple from scan
 */
bool
tp_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	TpScanOpaque so = (TpScanOpaque)scan->opaque;
	float4		 bm25_score;
	BlockNumber	 blknum;

	(void)dir; /* BM25 index only supports forward scan */

	Assert(scan != NULL);
	Assert(so != NULL);
	Assert(so->query_text != NULL);

	/* Execute scoring query if we haven't done so yet */
	if (so->result_ctids == NULL && !so->eof_reached)
	{
		/* Count index scan for pg_stat_user_indexes */
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		if (!tp_execute_scoring_query(scan))
		{
			so->eof_reached = true;
			return false;
		}
		/* Scoring query must have allocated result_ctids on success */
		if (so->result_ctids == NULL)
		{
			so->eof_reached = true;
			return false;
		}
	}

	/* Check if we've reached the end of current result set */
	if (so->current_pos >= so->result_count || so->eof_reached)
	{
		/*
		 * If result_count hit the internal limit, there may be more
		 * documents.  Double the limit and re-execute the scoring
		 * query, skipping already-returned results.
		 */
		if (!so->eof_reached && so->result_count > 0 &&
			so->result_count >= so->max_results_used &&
			so->max_results_used < TP_MAX_QUERY_LIMIT)
		{
			int old_count = so->result_count;
			int new_limit = so->max_results_used * 2;

			if (new_limit > TP_MAX_QUERY_LIMIT)
				new_limit = TP_MAX_QUERY_LIMIT;

			so->limit = new_limit;
			if (tp_execute_scoring_query(scan) && so->result_count > old_count)
			{
				/* Skip already-returned results */
				so->current_pos = old_count;
				/* Fall through to return next tuple */
			}
			else
			{
				so->eof_reached = true;
				return false;
			}
		}
		else
			return false;
	}

	Assert(so->scan_context != NULL);
	Assert(so->result_ctids != NULL);
	Assert(so->current_pos < so->result_count);

	Assert(ItemPointerIsValid(&so->result_ctids[so->current_pos]));

	/* Skip results with invalid block numbers */
	blknum = BlockIdGetBlockNumber(
			&(so->result_ctids[so->current_pos].ip_blkid));
	while (blknum == InvalidBlockNumber)
	{
		so->current_pos++;
		if (so->current_pos >= so->result_count)
			return false;
		blknum = BlockIdGetBlockNumber(
				&(so->result_ctids[so->current_pos].ip_blkid));
	}

	scan->xs_heaptid		= so->result_ctids[so->current_pos];
	scan->xs_recheck		= false;
	scan->xs_recheckorderby = false;

	/* Set ORDER BY distance value */
	if (scan->numberOfOrderBys > 0)
	{
		float4 raw_score;

		Assert(scan->numberOfOrderBys == 1);
		Assert(scan->xs_orderbyvals != NULL);
		Assert(scan->xs_orderbynulls != NULL);
		Assert(so->result_scores != NULL);

		/* Convert BM25 score to Datum (ensure negative for ASC sort) */
		raw_score				 = so->result_scores[so->current_pos];
		bm25_score				 = (raw_score > 0) ? -raw_score : raw_score;
		scan->xs_orderbyvals[0]	 = Float4GetDatum(bm25_score);
		scan->xs_orderbynulls[0] = false;

		/* Log BM25 score if enabled */
		elog(tp_log_scores ? NOTICE : DEBUG1,
			 "BM25 index scan: tid=(%u,%u), BM25_score=%.4f",
			 BlockIdGetBlockNumber(&scan->xs_heaptid.ip_blkid),
			 scan->xs_heaptid.ip_posid,
			 bm25_score);

		/* Cache score for stub function to retrieve */
		tp_cached_score = (float8)bm25_score;
	}

	/* Move to next position */
	so->current_pos++;

	return true;
}
