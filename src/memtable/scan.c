/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * scan.c - Memtable scan operations
 *
 * This module provides the search interface for scanning the memtable
 * (and segments) during index scans.
 */
#include <postgres.h>

#include <access/genam.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/relscan.h>
#include <access/tableam.h>
#include <catalog/index.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <storage/bufmgr.h>
#include <tsearch/ts_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include <varatt.h>

#include "index/limit.h"
#include "index/metapage.h"
#include "index/state.h"
#include "memtable/memtable.h"
#include "memtable/posting.h"
#include "memtable/scan.h"
#include "scoring/bm25.h"
#include "scoring/phrase.h"
#include "segment/segment.h"
#include "types/array.h"
#include "types/query_parser.h"
#include "types/vector.h"

/*
 * Search the memtable (and segments) for documents matching the query vector.
 * Returns true on success (results stored in scan opaque), false on failure.
 *
 * This is the main entry point called from am/scan.c during tp_gettuple.
 */
bool
tp_memtable_search(
		IndexScanDesc	   scan,
		TpLocalIndexState *index_state,
		TpVector		  *query_vector,
		TpIndexMetaPage	   metap)
{
	TpScanOpaque  so = (TpScanOpaque)scan->opaque;
	int			  max_results;
	int			  result_count = 0;
	float4		  k1_value;
	float4		  b_value;
	MemoryContext oldcontext;

	/* Extract terms and frequencies from query vector */
	char		 **query_terms;
	int32		  *query_frequencies;
	TpVectorEntry *entries_ptr;
	int			   entry_count;
	char		  *ptr;

	if (!so)
		return false;

	/* Use limit from scan state, fallback to GUC parameter */
	if (so->limit > 0)
		max_results = so->limit;
	else
		max_results = tp_default_limit;

	entry_count		  = query_vector->entry_count;
	query_terms		  = palloc(entry_count * sizeof(char *));
	query_frequencies = palloc(entry_count * sizeof(int32));
	entries_ptr		  = TPVECTOR_ENTRIES_PTR(query_vector);

	/* Parse the query vector entries */
	ptr = (char *)entries_ptr;
	for (int i = 0; i < entry_count; i++)
	{
		TpVectorEntry *entry = (TpVectorEntry *)ptr;
		char		  *term_str;

		/* Allocate on heap for query terms array */
		term_str = palloc(entry->lexeme_len + 1);
		memcpy(term_str, entry->lexeme, entry->lexeme_len);
		term_str[entry->lexeme_len] = '\0';

		/* Store the term string directly in query terms array */
		query_terms[i]		 = term_str;
		query_frequencies[i] = entry->frequency;

		ptr += sizeof(TpVectorEntry) + MAXALIGN(entry->lexeme_len);
	}

	/* Allocate result arrays in scan context */
	oldcontext		 = MemoryContextSwitchTo(so->scan_context);
	so->result_ctids = palloc(max_results * sizeof(ItemPointerData));
	/* Initialize to invalid TIDs for safety */
	memset(so->result_ctids, 0, max_results * sizeof(ItemPointerData));
	MemoryContextSwitchTo(oldcontext);

	/* Extract values from metap */
	Assert(metap != NULL);
	k1_value = metap->k1;
	b_value	 = metap->b;

	Assert(index_state != NULL);
	Assert(query_terms != NULL);
	Assert(query_frequencies != NULL);
	Assert(so->result_ctids != NULL);

	/* Score documents using the unified scoring function */
	result_count = tp_score_documents(
			index_state,
			scan->indexRelation,
			query_terms,
			query_frequencies,
			entry_count,
			k1_value,
			b_value,
			max_results,
			so->result_ctids,
			&so->result_scores);

	so->result_count	 = result_count;
	so->current_pos		 = 0;
	so->max_results_used = max_results;

	/* Free the query terms array and individual term strings */
	for (int i = 0; i < entry_count; i++)
		pfree(query_terms[i]);

	pfree(query_terms);
	pfree(query_frequencies);

	return result_count > 0;
}

/*
 * Collect positions for a single phrase token across ALL candidate
 * ctids in one pass. positions_2d[cand_idx] is set to a palloc'd
 * uint32 array and counts_2d[cand_idx] to the frequency when the
 * term appears at that candidate with positions; untouched (NULL/0)
 * otherwise. Caller must pre-zero both arrays.
 */
static void
tp_collect_positions_for_term_batch(
		TpLocalIndexState	  *index_state,
		Relation			   index_rel,
		BlockNumber			  *level_heads,
		const char			  *term,
		const ItemPointerData *sorted_targets,
		int					   num_targets,
		uint32				 **positions_2d,
		uint32				  *counts_2d)
{
	int level;

	/* Memtable first (wins for fresh docs), then each segment level */
	tp_memtable_collect_positions_batch(
			index_state,
			term,
			sorted_targets,
			num_targets,
			positions_2d,
			counts_2d);

	for (level = 0; level < TP_MAX_LEVELS; level++)
	{
		if (level_heads[level] == InvalidBlockNumber)
			continue;
		tp_segment_collect_positions_batch(
				index_rel,
				level_heads[level],
				term,
				sorted_targets,
				num_targets,
				positions_2d,
				counts_2d);
	}
}

/*
 * Stem a single token through the index's text_config.
 * Returns palloc'd cstring (may be empty).
 */
static char *
tp_stem_query_token(const char *token, Oid text_config_oid)
{
	Datum	   tsv_datum;
	TSVector   tsv;
	WordEntry *entries;
	char	  *lex_start;
	char	  *result;

	tsv_datum = DirectFunctionCall2(
			to_tsvector_byid,
			ObjectIdGetDatum(text_config_oid),
			PointerGetDatum(cstring_to_text(token)));
	tsv = DatumGetTSVector(tsv_datum);
	if (tsv->size == 0)
		return pstrdup("");
	entries	  = ARRPTR(tsv);
	lex_start = STRPTR(tsv);
	result	  = palloc(entries[0].len + 1);
	memcpy(result, lex_start + entries[0].pos, entries[0].len);
	result[entries[0].len] = '\0';
	return result;
}

/*
 * Post-filter BMW candidates by phrase verification.
 *
 * For each candidate CTID, try the index-positions fast path first
 * (memtable DSA positions + V6 segment positions). If any phrase
 * token's positions aren't available from the index, fall back to
 * fetching the heap tuple and re-tokenizing. Drop candidates that
 * fail any phrase.
 *
 * In-place compaction: result_ctids[] / result_scores[] are
 * rewritten with surviving candidates, and *result_count is updated.
 */
static void
tp_phrase_postfilter_candidates(
		IndexScanDesc			   scan,
		TpParsedQuery			  *pq,
		Oid						   text_config_oid,
		TpLocalIndexState		  *index_state,
		BlockNumber				  *level_heads,
		const TpIndexMetaPageData *metap,
		ItemPointerData			  *result_ctids,
		float4					  *result_scores,
		int						  *result_count_inout)
{
	IndexFetchTableData *fetch	   = NULL;
	TupleTableSlot		*slot	   = NULL;
	IndexInfo			*indexInfo = NULL;
	EState				*estate	   = NULL;
	ExprContext			*econtext  = NULL;
	Datum				 idx_values[INDEX_MAX_KEYS];
	bool				 idx_isnull[INDEX_MAX_KEYS];
	int					 in_count  = *result_count_inout;
	int					 out_count = 0;
	int					 i;
	bool				 indexed_is_array;
	bool				 heap_available	 = false;
	char			  ***stemmed_clauses = NULL;

	if (pq == NULL || in_count == 0)
		return;

	/*
	 * Pre-stem each PHRASE clause's tokens once. We reuse them per
	 * candidate when trying the index-positions fast path. PHRASE_PREFIX
	 * falls back to heap re-tokenize (prefix expansion over positions
	 * from index is a future optimization).
	 */
	stemmed_clauses = palloc0(pq->term_count * sizeof(char **));
	for (i = 0; i < pq->term_count; i++)
	{
		TpQueryTerm *qt = &pq->terms[i];
		int			 t;

		if (qt->kind != TP_QTERM_PHRASE)
			continue;

		stemmed_clauses[i] = palloc0(qt->phrase_token_count * sizeof(char *));
		for (t = 0; t < qt->phrase_token_count; t++)
		{
			stemmed_clauses[i][t] =
					tp_stem_query_token(qt->phrase_tokens[t], text_config_oid);
		}
	}

	/*
	 * Prefetch heap blocks for all candidate CTIDs.
	 *
	 * The post-filter falls back to heap re-fetch + re-tokenize in
	 * two cases: PHRASE_PREFIX clauses (always), and PHRASE clauses
	 * where any token's positions weren't resolvable via the index.
	 * On cold cache those fetches would otherwise be serialized
	 * synchronous I/O. Issuing PrefetchBuffer for every candidate
	 * block upfront lets Postgres overlap those reads with our CPU
	 * work (driven by effective_io_concurrency).
	 *
	 * Cost when fallback isn't triggered: ~40 wasted async I/O
	 * submissions. Cheap compared to the savings on cold-cache
	 * queries that DO fall back. Skip entirely when scan has no
	 * heap relation (shouldn't happen for index-scan path but
	 * defensive).
	 */
	if (scan->heapRelation != NULL)
	{
		bool any_fallback_likely = false;
		for (i = 0; i < pq->term_count && !any_fallback_likely; i++)
		{
			if (pq->terms[i].kind == TP_QTERM_PHRASE_PREFIX)
				any_fallback_likely = true;
		}

		if (any_fallback_likely)
		{
			for (i = 0; i < in_count; i++)
			{
				if (ItemPointerIsValid(&result_ctids[i]))
					PrefetchBuffer(
							scan->heapRelation,
							MAIN_FORKNUM,
							ItemPointerGetBlockNumber(&result_ctids[i]));
			}
		}
	}

	/* Heap-fetch scaffolding (only used on fallback) */
	if (scan->heapRelation != NULL)
	{
		heap_available = true;

		{
			AttrNumber attnum =
					scan->indexRelation->rd_index->indkey.values[0];
			Oid atttype;

			if (attnum > 0)
			{
				atttype = TupleDescAttr(
								  RelationGetDescr(scan->heapRelation),
								  attnum - 1)
								  ->atttypid;
				indexed_is_array = (atttype == TEXTARRAYOID);
			}
			else
			{
				indexed_is_array = false;
			}
		}

		indexInfo = BuildIndexInfo(scan->indexRelation);
		estate	  = CreateExecutorState();
		econtext  = GetPerTupleExprContext(estate);
		slot	  = MakeSingleTupleTableSlot(
				 RelationGetDescr(scan->heapRelation), &TTSOpsBufferHeapTuple);
		fetch = table_index_fetch_begin(scan->heapRelation);
	}
	else
	{
		indexed_is_array = false;
	}

	/*
	 * Batch position collection.
	 *
	 * Build a sorted array of candidate CTIDs once, then for each
	 * PHRASE clause × each stemmed token, walk the posting list ONCE
	 * to collect positions for every candidate in a single pass. This
	 * reduces buffer traversal from O(candidates * tokens * posting_list_size)
	 * to O(tokens * posting_list_size) — ~30× fewer buffer touches
	 * on typical top-10 phrase queries (measured on SciFact).
	 *
	 * Layout: batch_positions[clause_idx][token_idx][cand_idx] is the
	 * uint32 position array for (candidate, token) in this clause; 0
	 * count / NULL positions means that candidate-term pairing has no
	 * positions available via the index and triggers heap fallback.
	 *
	 * For PHRASE_PREFIX clauses we skip the batch (always fall back
	 * to heap re-tokenize today). Tracked in the "follow-up" section.
	 */
	ItemPointerData *sorted_cands	   = NULL;
	int				*cand_orig_idx	   = NULL;
	int				 num_valid_cands   = 0;
	uint32		 ****clause_positions  = NULL;
	uint32		  ***clause_counts	   = NULL;
	bool			 any_phrase_clause = false;

	for (i = 0; i < pq->term_count && !any_phrase_clause; i++)
		if (pq->terms[i].kind == TP_QTERM_PHRASE)
			any_phrase_clause = true;

	if (any_phrase_clause)
	{
		int j, k;

		/* Collect and sort unique valid ctids from result_ctids */
		sorted_cands  = palloc(in_count * sizeof(ItemPointerData));
		cand_orig_idx = palloc(in_count * sizeof(int));
		for (i = 0; i < in_count; i++)
		{
			if (ItemPointerIsValid(&result_ctids[i]))
			{
				sorted_cands[num_valid_cands]  = result_ctids[i];
				cand_orig_idx[num_valid_cands] = i;
				num_valid_cands++;
			}
		}

		/* Insertion sort by (ctid, orig_idx) — num_valid_cands ≈ 40 */
		for (j = 1; j < num_valid_cands; j++)
		{
			ItemPointerData kv = sorted_cands[j];
			int				ko = cand_orig_idx[j];
			int				m  = j - 1;
			while (m >= 0 && ItemPointerCompare(
									 (ItemPointer)&sorted_cands[m],
									 (ItemPointer)&kv) > 0)
			{
				sorted_cands[m + 1]	 = sorted_cands[m];
				cand_orig_idx[m + 1] = cand_orig_idx[m];
				m--;
			}
			sorted_cands[m + 1]	 = kv;
			cand_orig_idx[m + 1] = ko;
		}

		/* Allocate per-clause × per-token × per-candidate position/count
		 * matrices. Zero-initialized: unfilled slots stay NULL/0. */
		clause_positions = palloc0(pq->term_count * sizeof(uint32 ***));
		clause_counts	 = palloc0(pq->term_count * sizeof(uint32 **));
		for (j = 0; j < pq->term_count; j++)
		{
			TpQueryTerm *qt = &pq->terms[j];
			if (qt->kind != TP_QTERM_PHRASE)
				continue;
			clause_positions[j] = palloc0(
					qt->phrase_token_count * sizeof(uint32 **));
			clause_counts[j] = palloc0(
					qt->phrase_token_count * sizeof(uint32 *));
			for (k = 0; k < qt->phrase_token_count; k++)
			{
				clause_positions[j][k] = palloc0(
						num_valid_cands * sizeof(uint32 *));
				clause_counts[j][k] = palloc0(
						num_valid_cands * sizeof(uint32));
				/*
				 * Skip tokens that stemmed to empty — there's no
				 * indexed term to look up, so this phrase clause
				 * can't match via the fast path. The per-candidate
				 * verifier will observe empty positions and signal
				 * fallback.
				 */
				if (stemmed_clauses[j] == NULL ||
					stemmed_clauses[j][k] == NULL ||
					stemmed_clauses[j][k][0] == '\0')
					continue;

				tp_collect_positions_for_term_batch(
						index_state,
						scan->indexRelation,
						level_heads,
						stemmed_clauses[j][k],
						sorted_cands,
						num_valid_cands,
						clause_positions[j][k],
						clause_counts[j][k]);
			}
		}
	}

	for (i = 0; i < in_count; i++)
	{
		ItemPointerData ctid			  = result_ctids[i];
		bool			all_phrases_match = true;
		bool			fallback_needed	  = false;
		int				clause_idx;
		int				sorted_idx = -1;

		if (!ItemPointerIsValid(&ctid))
			continue;

		/* Locate this candidate in sorted_cands */
		if (any_phrase_clause)
		{
			int lo = 0, hi = num_valid_cands - 1;
			while (lo <= hi)
			{
				int mid = lo + (hi - lo) / 2;
				int cmp = ItemPointerCompare(
						(ItemPointer)&sorted_cands[mid], (ItemPointer)&ctid);
				if (cmp == 0)
				{
					sorted_idx = mid;
					break;
				}
				else if (cmp < 0)
					lo = mid + 1;
				else
					hi = mid - 1;
			}
		}

		for (clause_idx = 0; clause_idx < pq->term_count; clause_idx++)
		{
			TpQueryTerm	  *qt = &pq->terms[clause_idx];
			TpPhraseResult vr;

			/*
			 * For multi-column indexes, the dictionary stores field-
			 * tagged stems. Our fast-path collector uses untagged
			 * stems (per pre-6.1 convention), so the lookup would
			 * never match. Force heap fallback for PHRASE clauses in
			 * multi-col until the collector is taught to tag.
			 */
			if (metap != NULL && metap->num_fields > 1 &&
				(qt->kind == TP_QTERM_PHRASE ||
				 qt->kind == TP_QTERM_PHRASE_PREFIX))
			{
				fallback_needed = true;
				break;
			}

			if (qt->kind == TP_QTERM_PHRASE && sorted_idx >= 0)
			{
				int		 k;
				bool	 any_missing   = false;
				uint32 **pos_per_token = palloc(
						qt->phrase_token_count * sizeof(uint32 *));
				int *count_per_token = palloc(
						qt->phrase_token_count * sizeof(int));

				for (k = 0; k < qt->phrase_token_count; k++)
				{
					pos_per_token[k] =
							clause_positions[clause_idx][k][sorted_idx];
					count_per_token[k] = (int)
							clause_counts[clause_idx][k][sorted_idx];
					if (pos_per_token[k] == NULL || count_per_token[k] == 0)
						any_missing = true;
				}

				if (any_missing)
				{
					vr = TP_PHRASE_BAD_INPUT;
				}
				else
				{
					vr = tp_phrase_verify_positions(
							pos_per_token,
							count_per_token,
							qt->phrase_token_count);
				}

				pfree(pos_per_token);
				pfree(count_per_token);

				if (vr == TP_PHRASE_MATCH)
					continue;
				if (vr == TP_PHRASE_NO_MATCH)
				{
					all_phrases_match = false;
					break;
				}
				/* TP_PHRASE_BAD_INPUT → fall back to heap */
				fallback_needed = true;
				break;
			}
			else if (
					qt->kind == TP_QTERM_PHRASE_PREFIX ||
					qt->kind == TP_QTERM_PHRASE)
			{
				/* PHRASE_PREFIX always falls back; PHRASE with
				 * sorted_idx<0 (invalid ctid — shouldn't happen) */
				fallback_needed = true;
				break;
			}
		}

		if (fallback_needed)
		{
			bool  call_again = false;
			bool  fetched;
			text *doc_text = NULL;
			char *doc_cstr = NULL;

			if (!heap_available)
			{
				/*
				 * No heap relation on standalone-ish scans —
				 * we can't verify. Drop the candidate.
				 */
				continue;
			}

			fetched = table_index_fetch_tuple(
					fetch, &ctid, scan->xs_snapshot, slot, &call_again, NULL);
			if (!fetched)
				continue;

			econtext->ecxt_scantuple = slot;
			FormIndexDatum(indexInfo, slot, estate, idx_values, idx_isnull);

			all_phrases_match = true;
			for (clause_idx = 0; clause_idx < pq->term_count; clause_idx++)
			{
				TpQueryTerm	  *qt = &pq->terms[clause_idx];
				TpPhraseResult vr = TP_PHRASE_NO_MATCH;
				bool		   last_is_prefix;
				bool		   any_field_matched = false;
				int			   col_first, col_last, c;

				if (qt->kind != TP_QTERM_PHRASE &&
					qt->kind != TP_QTERM_PHRASE_PREFIX)
					continue;

				last_is_prefix = (qt->kind == TP_QTERM_PHRASE_PREFIX);

				/*
				 * Multi-col heap-fallback: pick the right column text
				 * per phrase clause. Field-qualified clauses target
				 * one column; unqualified clauses in multi-col indexes
				 * check every column and match if any field contains
				 * the phrase (implicit per-field OR).
				 */
				if (qt->field_name != NULL && metap->num_fields > 1)
				{
					int fidx = tp_metapage_find_field(metap, qt->field_name);
					if (fidx < 0)
					{
						all_phrases_match = false;
						break;
					}
					col_first = fidx;
					col_last  = fidx + 1;
				}
				else if (metap->num_fields > 1)
				{
					col_first = 0;
					col_last  = metap->num_fields;
				}
				else
				{
					col_first = 0;
					col_last  = 1;
				}

				for (c = col_first; c < col_last; c++)
				{
					text *col_text;
					char *col_cstr;

					if (idx_isnull[c])
						continue;
					if (indexed_is_array)
						col_text = tp_flatten_text_array(idx_values[c]);
					else
						col_text = DatumGetTextPP(idx_values[c]);
					col_cstr = text_to_cstring(col_text);

					vr = tp_phrase_verify_text(
							col_cstr,
							text_config_oid,
							qt->phrase_tokens,
							qt->phrase_token_count,
							last_is_prefix,
							tp_metapage_field_format(metap, c));
					pfree(col_cstr);
					if (vr == TP_PHRASE_MATCH)
					{
						any_field_matched = true;
						break;
					}
				}

				if (!any_field_matched)
				{
					all_phrases_match = false;
					break;
				}
			}

			(void)doc_text;
			(void)doc_cstr;
			ExecClearTuple(slot);
			ResetExprContext(econtext);
		}

		if (all_phrases_match)
		{
			result_ctids[out_count]	 = ctid;
			result_scores[out_count] = result_scores[i];
			out_count++;
		}
	}

	if (heap_available)
	{
		table_index_fetch_end(fetch);
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
	}

	/* Free batch-collected positions */
	if (clause_positions != NULL)
	{
		int j, k, c;
		for (j = 0; j < pq->term_count; j++)
		{
			if (clause_positions[j] == NULL)
				continue;
			for (k = 0; k < pq->terms[j].phrase_token_count; k++)
			{
				if (clause_positions[j][k] == NULL)
					continue;
				for (c = 0; c < num_valid_cands; c++)
				{
					if (clause_positions[j][k][c] != NULL)
						pfree(clause_positions[j][k][c]);
				}
				pfree(clause_positions[j][k]);
				pfree(clause_counts[j][k]);
			}
			pfree(clause_positions[j]);
			pfree(clause_counts[j]);
		}
		pfree(clause_positions);
		pfree(clause_counts);
	}
	if (sorted_cands)
		pfree(sorted_cands);
	if (cand_orig_idx)
		pfree(cand_orig_idx);

	/* Free pre-stemmed clauses */
	for (i = 0; i < pq->term_count; i++)
	{
		if (stemmed_clauses[i] != NULL)
		{
			int t;
			for (t = 0; t < pq->terms[i].phrase_token_count; t++)
			{
				if (stemmed_clauses[i][t])
					pfree(stemmed_clauses[i][t]);
			}
			pfree(stemmed_clauses[i]);
		}
	}
	pfree(stemmed_clauses);

	*result_count_inout = out_count;
}

bool
tp_memtable_search_with_grammar(
		IndexScanDesc	   scan,
		TpLocalIndexState *index_state,
		const char		  *query_text,
		TpIndexMetaPage	   metap,
		bool			   fuzzy,
		uint8			   fuzzy_max_distance)
{
	TpScanOpaque   so = (TpScanOpaque)scan->opaque;
	int			   max_results;
	int			   bmw_max_results;
	int			   result_count = 0;
	float4		   k1_value, b_value;
	MemoryContext  oldcontext;
	TpParsedQuery *pq;
	BlockNumber	   level_heads[TP_MAX_LEVELS];
	char		 **terms;
	int32		  *freqs;
	float4		  *weights = NULL;
	int			   term_count;
	int			   i;
	bool		   has_phrase;

	if (!so)
		return false;

	if (so->limit > 0)
		max_results = so->limit;
	else
		max_results = tp_default_limit;

	pq = tp_parse_query(query_text);
	if (pq == NULL || pq->term_count == 0)
	{
		if (pq)
			tp_free_parsed_query(pq);
		return false;
	}

	has_phrase = tp_parsed_query_has_phrase(pq);

	/*
	 * For phrase queries we run BMW on bag-of-words and post-filter
	 * via heap re-fetch. Overfetch by phrase_candidate_overfetch so
	 * the phrase-rare case still returns a reasonable result count.
	 */
	if (has_phrase)
	{
		int64 amplified = (int64)max_results *
						  (int64)tp_phrase_candidate_overfetch;
		if (amplified > TP_MAX_QUERY_LIMIT)
			amplified = TP_MAX_QUERY_LIMIT;
		bmw_max_results = (int)amplified;
		if (bmw_max_results < max_results)
			bmw_max_results = max_results;
	}
	else
	{
		bmw_max_results = max_results;
	}

	for (i = 0; i < TP_MAX_LEVELS; i++)
		level_heads[i] = metap->level_heads[i];

	term_count = tp_resolve_query_terms_ex(
			pq,
			index_state,
			scan->indexRelation,
			level_heads,
			metap->text_config_oid,
			metap,
			fuzzy,
			fuzzy_max_distance,
			&terms,
			&freqs,
			fuzzy ? &weights : NULL);

	if (term_count == 0)
	{
		/* Nothing to score — query expanded to empty set */
		oldcontext		 = MemoryContextSwitchTo(so->scan_context);
		so->result_ctids = palloc(max_results * sizeof(ItemPointerData));
		memset(so->result_ctids, 0, max_results * sizeof(ItemPointerData));
		so->result_scores = palloc(max_results * sizeof(float4));
		MemoryContextSwitchTo(oldcontext);
		so->result_count	 = 0;
		so->current_pos		 = 0;
		so->max_results_used = max_results;

		if (terms)
			pfree(terms);
		if (freqs)
			pfree(freqs);
		if (weights)
			pfree(weights);
		tp_free_parsed_query(pq);
		return false;
	}

	oldcontext		 = MemoryContextSwitchTo(so->scan_context);
	so->result_ctids = palloc(bmw_max_results * sizeof(ItemPointerData));
	memset(so->result_ctids, 0, bmw_max_results * sizeof(ItemPointerData));
	MemoryContextSwitchTo(oldcontext);

	Assert(metap != NULL);
	k1_value = metap->k1;
	b_value	 = metap->b;

	result_count = tp_score_documents_weighted(
			index_state,
			scan->indexRelation,
			terms,
			freqs,
			weights,
			term_count,
			k1_value,
			b_value,
			bmw_max_results,
			so->result_ctids,
			&so->result_scores);

	/*
	 * Post-filter for phrase verification when needed. BMW returns
	 * candidates by bag-of-words score; tp_phrase_postfilter_candidates
	 * compacts the array in place keeping only those whose phrases
	 * verify.
	 */
	if (has_phrase && result_count > 0)
	{
		tp_phrase_postfilter_candidates(
				scan,
				pq,
				metap->text_config_oid,
				index_state,
				level_heads,
				metap,
				so->result_ctids,
				so->result_scores,
				&result_count);
	}

	/* Clamp result_count to caller's requested max */
	if (result_count > max_results)
		result_count = max_results;

	so->result_count	 = result_count;
	so->current_pos		 = 0;
	so->max_results_used = bmw_max_results;

	for (i = 0; i < term_count; i++)
		pfree(terms[i]);
	pfree(terms);
	pfree(freqs);
	if (weights)
		pfree(weights);
	tp_free_parsed_query(pq);

	return result_count > 0;
}
