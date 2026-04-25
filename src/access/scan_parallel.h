/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * scan_parallel.h - DSM layout and helpers for parallel index scan
 *
 * The IndexAmRoutine parallel-scan callbacks
 * reserve a region of the parallel-context DSM for AM-specific state.
 * That region is laid out as TpParallelScan + per-worker heap slots.
 *
 * The leader populates the segment-roots array once at scan setup,
 * then workers claim indices via atomic next_segment_index, score
 * into their own slot, and bump workers_done.  No shared heap; the
 * leader merges per-worker slots after fan-out completes.
 */
#pragma once

#include <postgres.h>

#include <port/atomics.h>
#include <storage/block.h>
#include <storage/itemptr.h>

#include "constants.h"

/*
 * Bounds chosen to keep the DSM region under ~few MB even at the
 * tail (TP_PARALLEL_MAX_K * sizeof(score+ctid) per worker per term).
 * Estimate-time has to return a fixed size; callers exceeding any
 * bound fall back to serial execution.
 */
#define TP_PARALLEL_MAX_SEGMENTS 256
#define TP_PARALLEL_MAX_TERMS	 64
#define TP_PARALLEL_MAX_WORKERS	 8
#define TP_PARALLEL_MAX_K		 10000

/*
 * Per-(worker, segment-result) entry deposited into the DSM slot.
 * Mirrors what TpTopKHeap stores per slot internally (segment-result
 * variant — workers never score memtable so seg_block is always
 * valid, never InvalidBlockNumber).
 */
typedef struct TpParallelHeapEntry
{
	BlockNumber seg_block;
	uint32		doc_id;
	float4		score;
} TpParallelHeapEntry;

/*
 * Per-worker DSM slot.  Worker fills entries[0..count-1] with its
 * top-k after segment scoring, then bumps workers_done.  Leader
 * merges across all slots.
 */
typedef struct TpParallelWorkerSlot
{
	int					count;
	TpParallelHeapEntry entries[TP_PARALLEL_MAX_K];
} TpParallelWorkerSlot;

/*
 * Shared region laid down at the start of the AM-specific DSM area.
 * Workers reach this via OffsetToPointer(scan->parallel_scan,
 *     scan->parallel_scan->ps_offset[_am]).
 *
 * The leader populates everything except next_segment_index /
 * workers_done before launching workers.  After fan-out the leader
 * polls workers_done, then merges the per-worker slots.
 */
typedef struct TpParallelScan
{
	/*
	 * Leader-to-worker setup handshake.  0 = not ready (workers
	 * spin-wait), 1 = eligible (workers proceed), 2 = ineligible
	 * (workers return EOF).  Written once by leader, read by workers.
	 */
	pg_atomic_uint32 setup_done;

	/* Set by leader at init.  Workers read these only — never write. */
	int			num_segments;
	BlockNumber segment_roots[TP_PARALLEL_MAX_SEGMENTS];

	/* Top-k cap (the query's LIMIT).  Bounded by TP_PARALLEL_MAX_K. */
	int max_results;

	/* Single-term query state (eligibility requires nterms == 1). */
	char   query_term[NAMEDATALEN];
	float4 idf;
	float4 term_avgdl;
	float4 k1;
	float4 b;

	/*
	 * Work-stealing counter.  Workers fetch_add 1 to claim the next
	 * segment index; out-of-range indices mean "no more work."
	 * Initialized to 0 by leader (see tp_initparallelscan).  Both
	 * leader and workers consume from the same counter.
	 */
	pg_atomic_uint32 next_segment_index;

	/*
	 * Workers increment workers_attached on entry and workers_done
	 * after depositing results.  Leader waits until done == attached.
	 */
	pg_atomic_uint32 workers_attached;
	pg_atomic_uint32 workers_done;

	/* Per-worker output slots.  Indexed by ParallelWorkerNumber. */
	TpParallelWorkerSlot workers[TP_PARALLEL_MAX_WORKERS];
} TpParallelScan;

/*
 * Size needed for the AM-specific DSM region.  Doesn't depend on
 * nkeys / norderbys for our AM, but the callback signature takes
 * them so we can future-proof.
 */
extern Size tp_parallel_scan_size(void);
