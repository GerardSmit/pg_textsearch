/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * stringtable.h - String interning hash table using dshash
 */
#pragma once

#include <postgres.h>

#include <lib/dshash.h>
#include <storage/itemptr.h>
#include <storage/lwlock.h>
#include <utils/dsa.h>

#include "memtable/posting.h"
#include "types/fuzzy.h"

typedef struct TpStringHashEntry TpStringHashEntry;

/*
 * Key structure that supports both lookup via char* and storage via
 * dsa_pointer. posting_list is set to InvalidDsaPointer for lookup keys and
 * non-InvalidDsaPointer for table entry keys.  Tricky but avoids allocations
 * for lookups while saving space in the hash table.
 *
 * For lookup keys: term.str points to (possibly non-null-terminated) string,
 * len contains the string length.
 * For stored keys: term.dp points to null-terminated DSA string, len is
 * unused.
 */
typedef struct TpStringKey
{
	union
	{
		const char *str;
		dsa_pointer dp;
	} term;

	dsa_pointer posting_list;
	uint32		len; /* String length for lookup keys (avoids strlen) */
} TpStringKey;

/* Helper function to get the string from a key */
extern const char *tp_get_key_str(dsa_area *area, const TpStringKey *key);

/*
 * dshash entry structure for string interning and posting list mapping
 * Key distinguishes between local char* (for lookups) and DSA strings (for
 * storage)
 */
struct TpStringHashEntry
{
	/* Term plus a pointer to the posting list */
	TpStringKey key;
};

/* String table creation and initialization */
extern dshash_table *tp_string_table_create(dsa_area *area);
extern dshash_table *
tp_string_table_attach(dsa_area *area, dshash_table_handle handle);

/* Core hash table operations */
extern TpStringHashEntry *tp_string_table_lookup(
		dsa_area *area, dshash_table *ht, const char *str, size_t len);
extern TpStringHashEntry *tp_string_table_insert(
		dsa_area *area, dshash_table *ht, const char *str, size_t len);
extern void tp_string_table_clear(dsa_area *area, dshash_table *ht);

/* Ensure the string hash table is initialized (call under EXCLUSIVE) */
extern void tp_ensure_string_table_initialized(TpLocalIndexState *local_state);

/*
 * Document term management.
 *
 * positions is an optional array of position arrays — one entry per
 * term, each entry a uint32[frequencies[i]]. May be NULL when the
 * caller doesn't have positions (e.g. with_positions=false). Per-entry
 * NULL elements are also accepted.
 */
extern void tp_add_document_terms(
		TpLocalIndexState *local_state,
		ItemPointer		   ctid,
		char			 **terms,
		int32			  *frequencies,
		uint32			 **positions,
		int				   term_count,
		int32			   doc_length,
		const int32		  *field_lengths,
		int				   num_fields);

/* Posting list access via string table */
extern TpPostingList *tp_string_table_get_posting_list(
		dsa_area *area, dshash_table *ht, const char *term);

/*
 * Collect memtable terms starting with 'prefix', up to max_terms.
 * Output array and individual strings are palloc'd in the current
 * memory context. *out_terms is NULL when no matches.
 *
 * Iteration is O(num_terms) since dshash is unordered. The memtable
 * is bounded by spill thresholds (GUC pg_textsearch.memtable_spill_threshold)
 * so this is acceptable.
 */
extern void tp_memtable_collect_prefix_terms(
		TpLocalIndexState *local_state,
		const char		  *prefix,
		int				   max_terms,
		char			***out_terms,
		int				  *out_count);

extern void tp_memtable_collect_fuzzy_terms(
		TpLocalIndexState *local_state,
		const char		  *query_term,
		int				   max_distance,
		int				   max_terms,
		bool			   prefix,
		TpFuzzyCandidate **out_candidates,
		int				  *out_count);

/* Posting list management in DSA */
extern dsa_pointer tp_alloc_posting_list(dsa_area *dsa);
extern TpPostingEntry *
tp_alloc_posting_entries_dsa(dsa_area *area, uint32 capacity);

/* Helper functions for dsa_pointer conversion */
extern TpPostingList *
tp_get_posting_list_from_dp(dsa_area *area, dsa_pointer dp);
extern TpPostingEntry *
tp_get_posting_entries_from_dp(dsa_area *area, dsa_pointer dp);

/* LWLock tranche for string table locking */
#define TP_STRING_HASH_TRANCHE_ID LWTRANCHE_FIRST_USER_DEFINED
