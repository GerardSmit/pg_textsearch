/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * bmw.h - Block-Max WAND query optimization
 *
 * Implements early termination for top-k queries by computing upper bounds
 * on block scores and skipping blocks that cannot contribute to results.
 */
#pragma once

#include <postgres.h>

#include <storage/itemptr.h>
#include <utils/memutils.h>

#include "index/state.h"
#include "segment/io.h"
#include "segment/segment.h"

/*
 * Top-K min-heap for maintaining threshold during scoring.
 *
 * Heap property: parent score <= child scores (minimum at root).
 * This allows O(1) threshold access and O(log k) updates.
 * When heap is full, threshold = root score (minimum in top-k).
 *
 * Supports deferred CTID resolution: segment results store (seg_block, doc_id)
 * and resolve CTIDs only at extraction time. Memtable results have CTIDs
 * immediately (seg_block = InvalidBlockNumber).
 */
typedef struct TpTopKHeap
{
	ItemPointerData *ctids; /* Array of k CTIDs (resolved at extraction) */
	BlockNumber		*seg_blocks; /* Segment root block (InvalidBlockNumber =
									memtable) */
	uint32 *doc_ids;			 /* Segment-local doc IDs */
	float4 *scores;				 /* Parallel array of scores */
	int		capacity;			 /* k - maximum results */
	int		size;				 /* Current entries (0 to k) */
} TpTopKHeap;

/*
 * Initialize a top-k heap.
 * Allocates arrays in the given memory context.
 */
extern void tp_topk_init(TpTopKHeap *heap, int k, MemoryContext ctx);

/*
 * Free a top-k heap's allocated memory.
 */
extern void tp_topk_free(TpTopKHeap *heap);

/*
 * Get current threshold (minimum score to enter top-k).
 * Returns 0 if heap not yet full.
 */
static inline float4
tp_topk_threshold(TpTopKHeap *heap)
{
	return (heap->size >= heap->capacity) ? heap->scores[0] : 0.0f;
}

/*
 * Check if a score is definitely dominated (cannot enter top-k).
 * Quick check to avoid heap operations for non-competitive docs.
 * Returns false for equal scores since they may qualify via CTID tie-breaking.
 */
static inline bool
tp_topk_dominated(TpTopKHeap *heap, float4 score)
{
	return heap->size >= heap->capacity && score < heap->scores[0];
}

/*
 * Add a memtable result to the top-k heap.
 * CTID is known immediately for memtable entries.
 */
extern void
tp_topk_add_memtable(TpTopKHeap *heap, ItemPointerData ctid, float4 score);

/*
 * Add a segment result to the top-k heap.
 * CTID resolution is deferred until extraction.
 */
extern void tp_topk_add_segment(
		TpTopKHeap *heap, BlockNumber seg_block, uint32 doc_id, float4 score);

/*
 * Resolve CTIDs for segment results in the heap.
 * Must be called before tp_topk_extract.
 * Opens segments as needed to look up CTIDs.
 */
extern void tp_topk_resolve_ctids(TpTopKHeap *heap, Relation index);

/*
 * Extract sorted results from heap (descending by score).
 * Returns number of results extracted.
 * After extraction, heap is empty.
 * Note: Call tp_topk_resolve_ctids first if heap contains segment results.
 */
extern int
tp_topk_extract(TpTopKHeap *heap, ItemPointerData *ctids, float4 *scores);

/*
 * BMW statistics for debugging/EXPLAIN ANALYZE
 */
typedef struct TpBMWStats
{
	uint64 blocks_scanned; /* Segment blocks actually scored */
	uint64 blocks_skipped; /* Segment blocks skipped by BMW */
	uint64 memtable_docs;  /* Documents scored from memtable (exhaustive) */
	uint64 segment_docs_scored; /* Documents scored from segments */
	uint64 docs_in_results;		/* Documents in final results */

	uint64 seeks_performed;	  /* Binary search seeks executed */
	uint64 dead_docs_skipped; /* Dead docs filtered by alive bitset */
} TpBMWStats;

/*
 * Score documents using single-term Block-Max WAND.
 *
 * For single-term queries, uses block-level upper bounds to skip
 * blocks that cannot contribute to top-k results.
 *
 * Returns number of results (up to max_results).
 */
/*
 * Walk the segment chains rooted in metap->level_heads and return a
 * palloc'd array of segment-root BlockNumbers (across all levels).
 * Caller pfrees *out_roots.  Returns 0 when the index has no segments.
 *
 * Used by serial BMW to iterate; parallel-scan workers (parallel scan)
 * claim indices into the array via an atomic counter.
 */
extern int tp_collect_segment_roots(
		Relation index, TpIndexMetaPage metap, BlockNumber **out_roots);

/*
 * Per-term scoring state for multi-term BMW.  Public so parallel-
 * scan workers (parallel scan) can construct their own array per segment.
 */
typedef struct TpTermState
{
	const char *term;
	float4		idf;
	int32		query_freq; /* Query term frequency (for boosting) */

	/*
	 * Per-term BM25F state.
	 *   field_idx == -1 → untagged (single-col or unqualified
	 *                     in legacy index): use avg_doc_len = corpus avg.
	 *   field_idx >=  0 → tagged to that column: use avg_doc_len =
	 *                     metap->total_len_per_field[field_idx]/total_docs
	 */
	int	   field_idx;
	float4 avg_doc_len;

	/* Global maximum score across all blocks (for WAND pivot) */
	float4 max_score;

	/* Segment-specific state (reset per segment) */
	bool					 found; /* Term found in current segment */
	TpSegmentPostingIterator iter;	/* Iterator (contains dict_entry) */
	float4 *block_max_scores;		/* Pre-computed block max scores */
	uint32 *block_last_doc_ids;		/* Cached last_doc_id per block */
	uint32	cur_doc_id;				/* Cached current doc ID */
} TpTermState;

/*
 * Memtable scoring — exhaustive scan, no skip index.  Public so
 * parallel-scan leader can score memtable into its own heap.
 */
extern void score_memtable_single_term(
		TpTopKHeap		  *heap,
		TpLocalIndexState *local_state,
		const char		  *term,
		float4			   idf,
		float4			   k1,
		float4			   b,
		float4			   avg_doc_len,
		TpBMWStats		  *stats);

/*
 * Per-segment BMW scoring entrypoints — caller supplies an open
 * reader and a heap.  These are what parallel-scan workers will call
 * once they've claimed a segment from the work-stealing array.  The
 * serial path (tp_score_*_term_bmw) calls them too.
 */
extern void score_segment_single_term_bmw(
		TpTopKHeap		*heap,
		TpSegmentReader *reader,
		const char		*term,
		float4			 idf,
		float4			 k1,
		float4			 b,
		float4			 avg_doc_len,
		TpBMWStats		*stats);

extern void score_segment_multi_term_bmw(
		TpTopKHeap		*heap,
		TpSegmentReader *reader,
		TpTermState	   **terms,
		int				 term_count,
		float4			 k1,
		float4			 b,
		float4			 avg_doc_len,
		TpBMWStats		*stats);

extern int tp_score_single_term_bmw(
		TpLocalIndexState *local_state,
		Relation		   index,
		const char		  *term,
		float4			   idf,
		float4			   k1,
		float4			   b,
		float4			   avg_doc_len,
		int				   max_results,
		ItemPointerData	  *result_ctids,
		float4			  *result_scores,
		TpBMWStats		  *stats);

/*
 * Score documents using multi-term Block-Max WAND.
 *
 * For multi-term queries, uses the WAND algorithm with block-level
 * upper bounds to find top-k documents efficiently.
 *
 * Returns number of results (up to max_results).
 */
extern int tp_score_multi_term_bmw(
		TpLocalIndexState *local_state,
		Relation		   index,
		char			 **terms,
		int				   term_count,
		int32			  *query_freqs,
		float4			  *idfs,
		const float4	  *avg_doc_lens, /* per-term BM25F;
										  * NULL → uniform avg_doc_len */
		float4			 k1,
		float4			 b,
		float4			 avg_doc_len,
		int				 max_results,
		ItemPointerData *result_ctids,
		float4			*result_scores,
		TpBMWStats		*stats);

/*
 * Compute block maximum BM25 score from skip entry metadata.
 *
 * Uses block_max_tf and block_max_norm to compute upper bound on
 * any document's score in the block.
 */
extern float4 tp_compute_block_max_score(
		TpSkipEntry *skip,
		float4		 idf,
		float4		 k1,
		float4		 b,
		float4		 avg_doc_len);
