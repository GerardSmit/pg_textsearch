/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * posting.c - Posting list management for in-memory indexes
 */
#include <postgres.h>

#include <lib/dshash.h>
#include <math.h>
#include <miscadmin.h>
#include <storage/itemptr.h>
#include <utils/dsa.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

#include "common/hashfn.h"
#include "constants.h"
#include "index/metapage.h"
#include "index/registry.h"
#include "index/state.h"
#include "memtable/memtable.h"
#include "memtable/posting.h"
#include "memtable/stringtable.h"

/* Configuration parameters */
int tp_posting_list_growth_factor = TP_POSTING_LIST_GROWTH_FACTOR;

/*
 * Free a posting list, its entries array, and any per-entry
 * positions allocations.
 */
void
tp_free_posting_list(dsa_area *area, dsa_pointer posting_list_dp)
{
	TpPostingList *posting_list;

	if (!DsaPointerIsValid(posting_list_dp))
		return;

	posting_list = (TpPostingList *)dsa_get_address(area, posting_list_dp);

	if (DsaPointerIsValid(posting_list->entries_dp))
	{
		TpPostingEntry *entries = (TpPostingEntry *)
				dsa_get_address(area, posting_list->entries_dp);
		int32 i;

		for (i = 0; i < posting_list->doc_count; i++)
		{
			if (DsaPointerIsValid(entries[i].positions_dp))
				dsa_free(area, entries[i].positions_dp);
		}
		dsa_free(area, posting_list->entries_dp);
	}

	/* Free the posting list structure itself */
	dsa_free(area, posting_list_dp);
}

/* Helper function to get entries array from posting list */
TpPostingEntry *
tp_get_posting_entries(dsa_area *area, TpPostingList *posting_list)
{
	TpPostingEntry *entries;

	if (!posting_list || !DsaPointerIsValid(posting_list->entries_dp))
		return NULL;
	if (!area)
		return NULL;

	entries = dsa_get_address(area, posting_list->entries_dp);

#ifdef USE_ASSERT_CHECKING
	/*
	 * In debug builds, check if we're accessing freed memory.
	 * If memory was freed by tp_dsa_free, it will be filled with
	 * 0xDD sentinel pattern. Detecting this indicates use-after-free.
	 */
	if (entries && posting_list->doc_count > 0)
	{
		unsigned char *check = (unsigned char *)entries;
		bool		   looks_freed =
				(check[0] == 0xDD && check[1] == 0xDD && check[2] == 0xDD &&
				 check[3] == 0xDD);

		Assert(!looks_freed);
		if (looks_freed)
			elog(ERROR,
				 "use-after-free detected: accessing freed posting "
				 "list entries");
	}
#endif

	return entries;
}

/*
 * Allocate and initialize a new posting list in DSA
 * Returns the DSA pointer to the allocated posting list
 */
dsa_pointer
tp_alloc_posting_list(dsa_area *dsa)
{
	dsa_pointer	   posting_list_dp;
	TpPostingList *posting_list;

	Assert(dsa != NULL);

	posting_list_dp = dsa_allocate(dsa, sizeof(TpPostingList));
	if (!DsaPointerIsValid(posting_list_dp))
		elog(ERROR, "Failed to allocate posting list in DSA");

	posting_list = dsa_get_address(dsa, posting_list_dp);

	/* Initialize posting list */
	memset(posting_list, 0, sizeof(TpPostingList));
	LWLockInitialize(&posting_list->lock, TP_TRANCHE_POSTING_LOCK);
	posting_list->doc_count	 = 0;
	posting_list->capacity	 = 0;
	posting_list->is_sorted	 = false;
	posting_list->doc_freq	 = 0;
	posting_list->entries_dp = InvalidDsaPointer;

	return posting_list_dp;
}

/*
 * Add a document entry to a posting list - simplified
 */
void
tp_add_document_to_posting_list(
		TpLocalIndexState *local_state,
		TpPostingList	  *posting_list,
		ItemPointer		   ctid,
		int32			   frequency,
		const uint32	  *positions)
{
	TpPostingEntry *entries;
	TpPostingEntry *new_entry;
	dsa_pointer		new_entries_dp;
	dsa_pointer		positions_dp = InvalidDsaPointer;

	Assert(local_state != NULL);
	Assert(posting_list != NULL);
	Assert(ItemPointerIsValid(ctid));

	if (!local_state->is_build_mode)
		LWLockAcquire(&posting_list->lock, LW_EXCLUSIVE);

	/* Expand array if needed */
	if (posting_list->doc_count >= posting_list->capacity)
	{
		int32 new_capacity;
		Size  new_size;

		if (posting_list->capacity == 0)
			new_capacity = TP_INITIAL_POSTING_LIST_CAPACITY;
		else
		{
			/* Check for int32 overflow before multiplying */
			if (posting_list->capacity >
				INT32_MAX / tp_posting_list_growth_factor)
				elog(ERROR,
					 "posting list capacity overflow: %d * %d",
					 posting_list->capacity,
					 tp_posting_list_growth_factor);
			new_capacity = posting_list->capacity *
						   tp_posting_list_growth_factor;
		}
		new_size = (Size)new_capacity * sizeof(TpPostingEntry);

		new_entries_dp = dsa_allocate(local_state->dsa, new_size);
		if (!DsaPointerIsValid(new_entries_dp))
			elog(ERROR, "Failed to allocate posting entries in DSA");

		/* Copy existing entries if any */
		if (posting_list->doc_count > 0 &&
			DsaPointerIsValid(posting_list->entries_dp))
		{
			TpPostingEntry *old_entries =
					tp_get_posting_entries(local_state->dsa, posting_list);
			TpPostingEntry *new_entries =
					dsa_get_address(local_state->dsa, new_entries_dp);
			memcpy(new_entries,
				   old_entries,
				   posting_list->doc_count * sizeof(TpPostingEntry));

			dsa_free(local_state->dsa, posting_list->entries_dp);
		}

		posting_list->entries_dp = new_entries_dp;
		posting_list->capacity	 = new_capacity;
	}

	/* Allocate and copy positions if caller provided them */
	if (positions != NULL && frequency > 0)
	{
		uint32 *pos_buf;
		Size	pos_size = (Size)frequency * sizeof(uint32);

		positions_dp = dsa_allocate(local_state->dsa, pos_size);
		if (!DsaPointerIsValid(positions_dp))
			elog(ERROR, "Failed to allocate positions in DSA");
		pos_buf = (uint32 *)dsa_get_address(local_state->dsa, positions_dp);
		memcpy(pos_buf, positions, pos_size);
	}

	/* Add new document entry */
	entries			= tp_get_posting_entries(local_state->dsa, posting_list);
	new_entry		= &entries[posting_list->doc_count];
	new_entry->ctid = *ctid;
	new_entry->_pad = 0;
	new_entry->frequency	= frequency;
	new_entry->positions_dp = positions_dp;

	posting_list->doc_count++;
	posting_list->doc_freq	= posting_list->doc_count;
	posting_list->is_sorted = false; /* New entry may break sort order */

	if (!local_state->is_build_mode)
		LWLockRelease(&posting_list->lock);

	/* Track total postings for spill threshold */
	if (local_state->shared &&
		DsaPointerIsValid(local_state->shared->memtable_dp))
	{
		TpMemtable *memtable = dsa_get_address(
				local_state->dsa, local_state->shared->memtable_dp);
		pg_atomic_fetch_add_u64(&memtable->total_postings, 1);
	}
}

/*
 * Binary-search a sorted ItemPointerData array for the given key.
 * Returns the index on match, -1 on miss. Standard comparison is
 * by BlockNumber then OffsetNumber.
 */
static int
ctid_array_bsearch(
		const ItemPointerData *arr, int n, const ItemPointerData *key)
{
	int lo = 0;
	int hi = n - 1;
	while (lo <= hi)
	{
		int mid = lo + (hi - lo) / 2;
		int cmp = ItemPointerCompare((ItemPointer)&arr[mid], (ItemPointer)key);
		if (cmp == 0)
			return mid;
		else if (cmp < 0)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return -1;
}

void
tp_memtable_collect_positions_batch(
		TpLocalIndexState	  *local_state,
		const char			  *term,
		const ItemPointerData *sorted_targets,
		int					   num_targets,
		uint32				 **out_positions,
		uint32				  *out_counts)
{
	TpPostingList  *posting_list;
	TpPostingEntry *entries;
	int32			i;

	if (local_state == NULL || term == NULL || num_targets == 0)
		return;

	posting_list = tp_get_posting_list(local_state, term);
	if (posting_list == NULL || posting_list->doc_count == 0 ||
		!DsaPointerIsValid(posting_list->entries_dp))
		return;

	entries = (TpPostingEntry *)
			dsa_get_address(local_state->dsa, posting_list->entries_dp);

	for (i = 0; i < posting_list->doc_count; i++)
	{
		int	   idx;
		uint32 freq;

		idx = ctid_array_bsearch(
				sorted_targets, num_targets, &entries[i].ctid);
		if (idx < 0)
			continue;

		/* Already collected from another match-path? Skip. */
		if (out_positions[idx] != NULL)
			continue;

		freq = (uint32)entries[i].frequency;
		if (freq == 0 || !DsaPointerIsValid(entries[i].positions_dp))
			continue;

		out_positions[idx] = palloc(freq * sizeof(uint32));
		memcpy(out_positions[idx],
			   dsa_get_address(local_state->dsa, entries[i].positions_dp),
			   freq * sizeof(uint32));
		out_counts[idx] = freq;
	}
}

uint32
tp_memtable_read_positions_for_ctid(
		TpLocalIndexState *local_state,
		const char		  *term,
		ItemPointerData	   target_ctid,
		uint32			 **out_positions)
{
	TpPostingList  *posting_list;
	TpPostingEntry *entries;
	int32			i;

	*out_positions = NULL;

	if (local_state == NULL || term == NULL)
		return 0;

	posting_list = tp_get_posting_list(local_state, term);
	if (posting_list == NULL || posting_list->doc_count == 0 ||
		!DsaPointerIsValid(posting_list->entries_dp))
		return 0;

	entries = (TpPostingEntry *)
			dsa_get_address(local_state->dsa, posting_list->entries_dp);

	/*
	 * Posting entries are sorted by ctid only after finalization.
	 * The memtable is live — we linear scan. Posting lists are
	 * bounded by the spill threshold so this is O(doc_count) per
	 * lookup, acceptable at phrase-verify scale (~tens of candidates).
	 */
	for (i = 0; i < posting_list->doc_count; i++)
	{
		if (ItemPointerEquals(&entries[i].ctid, &target_ctid))
		{
			uint32 freq = (uint32)entries[i].frequency;

			if (freq == 0 || !DsaPointerIsValid(entries[i].positions_dp))
				return 0;

			*out_positions = palloc(freq * sizeof(uint32));
			memcpy(*out_positions,
				   dsa_get_address(local_state->dsa, entries[i].positions_dp),
				   freq * sizeof(uint32));
			return freq;
		}
	}
	return 0;
}

/*
 * Hash function for document length entries (CTID-based)
 */
static dshash_hash
tp_doclength_hash_function(const void *key, size_t keysize, void *arg)
{
	const ItemPointer ctid = (const ItemPointer)key;

	Assert(keysize == sizeof(ItemPointerData));
	(void)keysize;
	(void)arg;

	/* Hash both block number and offset */
	return hash_bytes((const unsigned char *)ctid, sizeof(ItemPointerData));
}

/*
 * Compare function for document length entries (CTID comparison)
 */
static int
tp_doclength_compare_function(
		const void *a, const void *b, size_t keysize, void *arg)
{
	const ItemPointer ctid_a = (const ItemPointer)a;
	const ItemPointer ctid_b = (const ItemPointer)b;

	Assert(keysize == sizeof(ItemPointerData));
	(void)keysize;
	(void)arg;

	return ItemPointerCompare(ctid_a, ctid_b);
}

/*
 * Copy function for document length entries (CTID copy)
 */
static void
tp_doclength_copy_function(
		void *dest, const void *src, size_t keysize, void *arg)
{
	Assert(keysize == sizeof(ItemPointerData));
	(void)keysize;
	(void)arg;

	*((ItemPointer)dest) = *((const ItemPointer)src);
}

/*
 * Create document length hash table
 */
dshash_table *
tp_doclength_table_create(dsa_area *area)
{
	dshash_parameters params;

	params.key_size			= sizeof(ItemPointerData);
	params.entry_size		= sizeof(TpDocLengthEntry);
	params.hash_function	= tp_doclength_hash_function;
	params.compare_function = tp_doclength_compare_function;
	params.copy_function	= tp_doclength_copy_function;
	params.tranche_id		= TP_DOCLENGTH_HASH_TRANCHE_ID;

	return dshash_create(area, &params, area);
}

/*
 * Attach to existing document length hash table
 */
dshash_table *
tp_doclength_table_attach(dsa_area *area, dshash_table_handle handle)
{
	dshash_parameters params;

	params.key_size			= sizeof(ItemPointerData);
	params.entry_size		= sizeof(TpDocLengthEntry);
	params.hash_function	= tp_doclength_hash_function;
	params.compare_function = tp_doclength_compare_function;
	params.copy_function	= tp_doclength_copy_function;
	params.tranche_id		= TP_DOCLENGTH_HASH_TRANCHE_ID;

	return dshash_attach(area, &params, handle, area);
}

/*
 * Store document length in the document length hash table
 */
void
tp_store_document_length(
		TpLocalIndexState *local_state, ItemPointer ctid, int32 doc_length)
{
	TpMemtable		 *memtable;
	dshash_table	 *doclength_table;
	TpDocLengthEntry *entry;
	bool			  found;

	Assert(local_state != NULL);
	Assert(ctid != NULL);

	memtable = get_memtable(local_state);
	if (!memtable)
		elog(ERROR, "Cannot get memtable - index state corrupted");

	/* Initialize document length table if needed */
	if (memtable->doc_lengths_handle == DSHASH_HANDLE_INVALID)
	{
		doclength_table = tp_doclength_table_create(local_state->dsa);
		memtable->doc_lengths_handle = dshash_get_hash_table_handle(
				doclength_table);
	}
	else
	{
		doclength_table = tp_doclength_table_attach(
				local_state->dsa, memtable->doc_lengths_handle);
	}

	/* Insert or update the document length */
	entry = (TpDocLengthEntry *)
			dshash_find_or_insert(doclength_table, ctid, &found);
	entry->ctid		  = *ctid;
	entry->doc_length = doc_length;

	dshash_release_lock(doclength_table, entry);
	dshash_detach(doclength_table);
}

/*
 * Phase 6.1d: per-field doc length table.  Same key (CTID), wider
 * entry (TpDocFieldLengthEntry).  Reused functions for hash/compare/copy.
 */
dshash_table *
tp_doc_field_length_table_create(dsa_area *area)
{
	dshash_parameters params;

	params.key_size			= sizeof(ItemPointerData);
	params.entry_size		= sizeof(TpDocFieldLengthEntry);
	params.hash_function	= tp_doclength_hash_function;
	params.compare_function = tp_doclength_compare_function;
	params.copy_function	= tp_doclength_copy_function;
	params.tranche_id		= TP_DOCLENGTH_HASH_TRANCHE_ID;

	return dshash_create(area, &params, area);
}

dshash_table *
tp_doc_field_length_table_attach(dsa_area *area, dshash_table_handle handle)
{
	dshash_parameters params;

	params.key_size			= sizeof(ItemPointerData);
	params.entry_size		= sizeof(TpDocFieldLengthEntry);
	params.hash_function	= tp_doclength_hash_function;
	params.compare_function = tp_doclength_compare_function;
	params.copy_function	= tp_doclength_copy_function;
	params.tranche_id		= TP_DOCLENGTH_HASH_TRANCHE_ID;

	return dshash_attach(area, &params, handle, area);
}

void
tp_store_document_field_lengths(
		TpLocalIndexState *local_state,
		ItemPointer		   ctid,
		int32			   doc_length,
		const int32		  *field_lengths,
		int				   num_fields)
{
	TpMemtable			  *memtable;
	dshash_table		  *fl_table;
	TpDocFieldLengthEntry *entry;
	bool				   found;
	int					   n;
	int					   f;

	Assert(local_state != NULL);
	Assert(ctid != NULL);
	Assert(field_lengths != NULL);
	Assert(num_fields > 0);

	memtable = get_memtable(local_state);
	if (!memtable)
		elog(ERROR, "Cannot get memtable - index state corrupted");

	if (memtable->doc_field_lengths_handle == DSHASH_HANDLE_INVALID)
	{
		fl_table = tp_doc_field_length_table_create(local_state->dsa);
		memtable->doc_field_lengths_handle = dshash_get_hash_table_handle(
				fl_table);
	}
	else
	{
		fl_table = tp_doc_field_length_table_attach(
				local_state->dsa, memtable->doc_field_lengths_handle);
	}

	entry = (TpDocFieldLengthEntry *)
			dshash_find_or_insert(fl_table, ctid, &found);
	entry->ctid		  = *ctid;
	entry->doc_length = doc_length;
	memset(entry->field_lengths, 0, sizeof(entry->field_lengths));
	n = num_fields < TP_MAX_FIELDS ? num_fields : TP_MAX_FIELDS;
	for (f = 0; f < n; f++)
		entry->field_lengths[f] = field_lengths[f];

	dshash_release_lock(fl_table, entry);
	dshash_detach(fl_table);
}

int32
tp_get_document_field_length_attached(
		dshash_table *fl_table, ItemPointer ctid, int field_idx)
{
	TpDocFieldLengthEntry *entry;

	Assert(fl_table != NULL);
	Assert(ctid != NULL);

	entry = (TpDocFieldLengthEntry *)dshash_find(fl_table, ctid, false);
	if (entry)
	{
		int32 v;

		if (field_idx < 0 || field_idx >= TP_MAX_FIELDS)
			v = entry->doc_length;
		else
			v = entry->field_lengths[field_idx];
		dshash_release_lock(fl_table, entry);
		return v;
	}
	return -1;
}

/*
 * Get document length using a pre-attached doclength table.
 * This is the optimized version for bulk lookups - avoids repeated
 * dshash_attach/detach overhead.
 */
int32
tp_get_document_length_attached(
		dshash_table *doclength_table, ItemPointer ctid)
{
	TpDocLengthEntry *entry;

	Assert(doclength_table != NULL);
	Assert(ctid != NULL);

	entry = (TpDocLengthEntry *)dshash_find(doclength_table, ctid, false);
	if (entry)
	{
		int32 doc_length = entry->doc_length;
		dshash_release_lock(doclength_table, entry);
		return doc_length;
	}
	return -1; /* Not found */
}

/*
 * Get document length from the document length hash table.
 * Falls back to searching segments if not found in memtable.
 */
int32
tp_get_document_length(
		TpLocalIndexState *local_state,
		Relation index	   pg_attribute_unused(),
		ItemPointer		   ctid)
{
	TpMemtable	 *memtable;
	dshash_table *doclength_table;
	int32		  doc_length;

	Assert(local_state != NULL);
	Assert(ctid != NULL);

	memtable = get_memtable(local_state);
	if (!memtable)
		elog(ERROR, "Cannot get memtable - index state corrupted");

	if (memtable->doc_lengths_handle == DSHASH_HANDLE_INVALID)
		return -1; /* Not in memtable, may be in segment */

	/* Attach to document length table */
	doclength_table = tp_doclength_table_attach(
			local_state->dsa, memtable->doc_lengths_handle);

	/* Look up the document length using the attached table */
	doc_length = tp_get_document_length_attached(doclength_table, ctid);

	dshash_detach(doclength_table);
	return doc_length;
}

/*
 * Shared memory cleanup - simplified stub
 */
