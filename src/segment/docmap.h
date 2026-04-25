/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * docmap.h - Document ID mapping for segment format
 *
 * Posting lists use compact 4-byte segment-local doc IDs
 * instead of 6-byte CTIDs. This module provides:
 * - Collection of unique documents during segment build
 * - Assignment of sequential doc IDs (0 to N-1) in CTID order
 * - CTID → doc_id lookup during posting conversion
 * - Arrays for CTID map and fieldnorm table
 *
 * INVARIANT: Doc IDs are assigned in CTID order after finalize.
 * This means CTID order = doc_id order, so postings sorted by CTID
 * are also sorted by doc_id, enabling sequential access to CTID arrays.
 */
#pragma once

#include "constants.h"
#include "postgres.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"

/*
 * Entry in the CTID → doc_id hash table (build-time only).
 * Used to quickly look up a document's assigned ID when converting postings.
 *
 * Phase 6.1d: field_lengths[] carries per-column lengths for multi-col
 * indexes.  Single-col docmaps populated via tp_docmap_add() leave the
 * array zeroed; the segment writer never reads it for untagged terms.
 */
typedef struct TpDocMapEntry
{
	ItemPointerData ctid;		/* Key: heap tuple location */
	uint32			doc_id;		/* Value: segment-local doc ID */
	uint32			doc_length; /* Document length (for fieldnorm) */
	uint32			field_lengths[TP_MAX_FIELDS]; /* Per-column lengths
												   * (multi-col only) */
} TpDocMapEntry;

/*
 * Document map builder context.
 * Collects documents and assigns sequential IDs during segment write.
 */
typedef struct TpDocMapBuilder
{
	HTAB  *ctid_to_id; /* Hash table: CTID → doc_id */
	uint32 num_docs;   /* Number of documents assigned */
	uint32 capacity;   /* Current capacity of arrays */
	bool   finalized;  /* True after tp_docmap_finalize called */

	/* Output arrays (indexed by doc_id, valid after finalize) */
	BlockNumber	 *ctid_pages;	/* doc_id → page number (4 bytes) */
	OffsetNumber *ctid_offsets; /* doc_id → tuple offset (2 bytes) */
	uint8		 *fieldnorms;	/* doc_id → encoded total length (1 byte) */

	/*
	 * Phase 6.1d: per-field encoded fieldnorm table.  Allocated
	 * iff num_fields > 1.  Indexed field_fieldnorms[doc_id * num_fields + f].
	 * Set up by tp_docmap_set_num_fields() before tp_docmap_finalize().
	 * Single-col / merge-built docmaps leave it NULL — accessor falls
	 * back to the per-doc total fieldnorm.
	 */
	uint8 *field_fieldnorms;
	int	   num_fields; /* 0 / 1 → single-col, no per-field norms */

	/* Sum of per-doc token counts; used for segment.total_tokens. */
	uint64 total_tokens;

	/*
	 * Phase 6.1d: Σ per-doc field_lengths[f] across docs added to this
	 * builder.  Used by spill sites to roll into metapage->total_len_per_field
	 * after the segment is written.  Stays at zero for merge-built
	 * docmaps (merge derives totals from source segment metapage rolls).
	 */
	uint64 total_tokens_per_field[TP_MAX_FIELDS];
} TpDocMapBuilder;

/*
 * Create a new document map builder (constructor).
 * Call this before collecting documents.
 */
extern TpDocMapBuilder *tp_docmap_create(void);

/*
 * Add a document to the map.
 * Returns the assigned doc_id (reuses existing ID if CTID already present).
 * doc_length is stored for fieldnorm encoding; if CTID already exists, the
 * original doc_length is kept (callers should ensure consistent lengths).
 */
extern uint32
tp_docmap_add(TpDocMapBuilder *builder, ItemPointer ctid, uint32 doc_length);

/*
 * Phase 6.1d: multi-col variant.  field_lengths must be an array of
 * num_fields uint32s whose sum equals doc_length.  num_fields <= 1
 * delegates to tp_docmap_add (single-col semantics).  Caller is
 * responsible for having called tp_docmap_set_num_fields() first.
 */
extern uint32 tp_docmap_add_multi(
		TpDocMapBuilder *builder,
		ItemPointer		 ctid,
		uint32			 doc_length,
		const uint32	*field_lengths,
		int				 num_fields);

/*
 * Phase 6.1d: enable per-field fieldnorm tracking.  Must be called
 * before tp_docmap_finalize when num_fields > 1.  No-op for <= 1.
 */
extern void tp_docmap_set_num_fields(TpDocMapBuilder *builder, int num_fields);

/*
 * Look up doc_id for a CTID using hash table.
 * Returns UINT32_MAX if not found.
 */
extern uint32 tp_docmap_lookup(TpDocMapBuilder *builder, ItemPointer ctid);

/*
 * Finalize the document map.
 * Builds the ctid arrays and fieldnorms array sorted by doc_id.
 * After this call, the hash table is no longer needed.
 */
extern void tp_docmap_finalize(TpDocMapBuilder *builder);

/*
 * Free the document map builder and all associated memory.
 */
extern void tp_docmap_destroy(TpDocMapBuilder *builder);

/*
 * Get the CTID for a doc_id. Requires finalize to have been called.
 * Reconstructs ItemPointerData from the split storage.
 */
static inline void
tp_docmap_get_ctid(TpDocMapBuilder *builder, uint32 doc_id, ItemPointer result)
{
	Assert(builder->finalized);
	Assert(doc_id < builder->num_docs);
	ItemPointerSet(
			result,
			builder->ctid_pages[doc_id],
			builder->ctid_offsets[doc_id]);
}

/*
 * Get the fieldnorm for a doc_id. Requires finalize to have been called.
 */
static inline uint8
tp_docmap_get_fieldnorm(TpDocMapBuilder *builder, uint32 doc_id)
{
	Assert(builder->finalized);
	if (doc_id >= builder->num_docs)
		return 0;
	return builder->fieldnorms[doc_id];
}

/*
 * Phase 6.1d: per-field encoded fieldnorm.  Falls back to the total
 * fieldnorm when this builder has no per-field table (single-col or
 * merge-built) or when field_idx is out of range.
 */
static inline uint8
tp_docmap_get_field_fieldnorm(
		TpDocMapBuilder *builder, uint32 doc_id, int field_idx)
{
	Assert(builder->finalized);
	if (doc_id >= builder->num_docs)
		return 0;
	if (builder->field_fieldnorms == NULL || builder->num_fields <= 1 ||
		field_idx < 0 || field_idx >= builder->num_fields)
		return builder->fieldnorms[doc_id];
	return builder->field_fieldnorms[doc_id * builder->num_fields + field_idx];
}
