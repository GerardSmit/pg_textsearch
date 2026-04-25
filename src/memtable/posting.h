/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * posting.h - In-memory posting list structures for DSA shared memory
 */
#pragma once

#include <postgres.h>

#include <storage/lwlock.h>
#include <storage/spin.h>
#include <utils/dsa.h>
#include <utils/hsearch.h>

#include "index/state.h"
#include "memtable/posting_entry.h"

/*
 * Posting list for a single term
 * Uses dynamic arrays with O(1) amortized inserts during building,
 * then sorts once at finalization for optimal query performance
 */
typedef struct TpPostingList
{
	LWLock		lock;		/* Per-posting-list concurrency */
	int32		doc_count;	/* Length of the entries array */
	int32		capacity;	/* Allocated array capacity */
	bool		is_sorted;	/* True after final sort for queries */
	int32		doc_freq;	/* Document frequency (for IDF calculation) */
	dsa_pointer entries_dp; /* DSA pointer to TpPostingEntry array */
} TpPostingList;

/* Array growth multiplier */
extern int tp_posting_list_growth_factor;

/* Posting list memory management */
extern void tp_free_posting_list(dsa_area *area, dsa_pointer posting_list_dp);
extern TpPostingEntry *
tp_get_posting_entries(dsa_area *area, TpPostingList *posting_list);

/*
 * Append a doc's posting to a term's posting list, optionally with
 * a position list. positions may be NULL (no positions captured for
 * this term) — frequency is still recorded. When non-NULL, the
 * caller-owned uint32[frequency] array is copied into a fresh DSA
 * allocation owned by the entry.
 */
extern void tp_add_document_to_posting_list(
		TpLocalIndexState *local_state,
		TpPostingList	  *posting_list,
		ItemPointer		   ctid,
		int32			   frequency,
		const uint32	  *positions);

/*
 * Memtable positions lookup. Linearly scan the given posting list
 * for an entry matching target_ctid and (if found with captured
 * positions) copy them into a fresh palloc'd buffer.
 *
 * Returns the positions count (== term frequency in that doc). 0
 * when the term isn't in the memtable, the ctid isn't in its
 * posting list, or the entry has no positions_dp.
 *
 * *out_positions is set NULL on zero return.
 */
extern uint32 tp_memtable_read_positions_for_ctid(
		TpLocalIndexState *local_state,
		const char		  *term,
		ItemPointerData	   target_ctid,
		uint32			 **out_positions);

/*
 * Batch memtable positions lookup. Walks the term's posting list
 * ONCE; for each posting whose ctid is in sorted_targets[], records
 * the positions (palloc'd) at the matching index of out_positions[]
 * / out_counts[].
 *
 * sorted_targets: array of ItemPointerData, sorted ascending.
 * out_positions[i]: palloc'd on match, untouched on miss.
 * out_counts[i]:    set on match, untouched on miss.
 *
 * Caller pre-initializes out_positions[] to NULL and out_counts[]
 * to 0, then runs batches per-term to accumulate positions across
 * multiple phrase tokens.
 */
extern void tp_memtable_collect_positions_batch(
		TpLocalIndexState	  *local_state,
		const char			  *term,
		const ItemPointerData *sorted_targets,
		int					   num_targets,
		uint32				 **out_positions,
		uint32				  *out_counts);

/* Document length hash table tranche ID */
#define TP_DOCLENGTH_HASH_TRANCHE_ID (LWTRANCHE_FIRST_USER_DEFINED + 1)

/* Document length hash table operations */
extern void tp_store_document_length(
		TpLocalIndexState *local_state, ItemPointer ctid, int32 doc_length);

extern int32 tp_get_document_length(
		TpLocalIndexState *local_state, Relation index, ItemPointer ctid);

/*
 * Get document length using a pre-attached doclength table.
 * This avoids repeated dshash_attach/detach overhead when looking up
 * multiple document lengths.
 */
extern int32 tp_get_document_length_attached(
		dshash_table *doclength_table, ItemPointer ctid);

extern dshash_table *tp_doclength_table_create(dsa_area *area);
extern dshash_table *
tp_doclength_table_attach(dsa_area *area, dshash_table_handle handle);

/*
 * Phase 6.1d: per-field doc length table operations.  Used only for
 * multi-col indexes; single-col indexes never call these.  Stores
 * TpDocFieldLengthEntry which carries both the total doc_length and
 * per-column field_lengths[].
 */
extern dshash_table *tp_doc_field_length_table_create(dsa_area *area);
extern dshash_table *
tp_doc_field_length_table_attach(dsa_area *area, dshash_table_handle handle);

extern void tp_store_document_field_lengths(
		TpLocalIndexState *local_state,
		ItemPointer		   ctid,
		int32			   doc_length,
		const int32		  *field_lengths,
		int				   num_fields);

extern int32 tp_get_document_field_length_attached(
		dshash_table *fl_table, ItemPointer ctid, int field_idx);

/* Index building operations */
extern float4 tp_calculate_idf(int32 doc_freq, int32 total_docs);

/* Shared memory cleanup */
extern void tp_cleanup_index_shared_memory(Oid index_oid);
