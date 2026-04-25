/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * metapage.h - Index metapage structures and operations
 */
#pragma once

#include <postgres.h>

#include <storage/block.h>
#include <storage/bufpage.h>
#include <storage/itemptr.h>
#include <utils/rel.h>

#include "constants.h"

typedef struct TpLocalIndexState TpLocalIndexState;

/*
 * Tapir Index Metapage Structure
 *
 * The metapage is stored on block 0 of every Tapir index and contains
 * configuration parameters and global statistics needed for BM25 scoring.
 *
 * Segment hierarchy: LSM-style tiered compaction with TP_MAX_LEVELS levels.
 * Level 0 receives segments from memtable spills (~8MB each).
 * When a level reaches segments_per_level segments, they are merged into
 * a single segment at the next level. This provides exponentially larger
 * segments at higher levels while bounding write amplification.
 */
typedef struct TpIndexMetaPageData
{
	uint32 magic;			/* Magic number for validation */
	uint32 version;			/* Index format version */
	Oid	   text_config_oid; /* Text search configuration OID */
	/*
	 * Corpus size for BM25 scoring.  Satisfies the exact invariant
	 *
	 *     total_docs = Σ segment.num_docs
	 *
	 * where the sum is over all on-disk segments reachable from
	 * level_heads[] and num_docs is the segment header field (fixed
	 * at segment creation).  VACUUM keeps the invariant by
	 * decrementing total_docs whenever a segment's num_docs changes
	 * or the segment leaves the chain: pre-V5 rebuild shrinks
	 * num_docs from old to docs_added; a V5 segment dropped because
	 * all docs are dead contributes zero.  V5 bitset flips that
	 * leave survivors are invisible here — they shrink alive_count,
	 * not num_docs.
	 *
	 * Per-segment dictionary doc_freq(t) ≤ segment.num_docs by
	 * construction, so
	 *
	 *     total_docs = Σ segment.num_docs
	 *              ≥ Σ segment.doc_freq(t)
	 *              = doc_freq(t) globally
	 *
	 * and BM25's N ≥ df(t) precondition for tp_calculate_idf holds.
	 *
	 * The shared-memory atomic additionally counts unflushed
	 * memtable docs and is the source persisted at every spill;
	 * this disk copy therefore lags between spills but is still in
	 * sync with Σ segment.num_docs at spill/sync boundaries.
	 */
	uint64 total_docs;
	uint64 _unused_total_terms;	  /* Unused, retained for on-disk compat */
	uint64 total_len;			  /* Σ segment.total_len, same frame as
									 total_docs */
	float4		k1;				  /* BM25 k1 parameter */
	float4		b;				  /* BM25 b parameter */
	BlockNumber root_blkno;		  /* Root page of the index tree */
	BlockNumber term_stats_root;  /* Root page of term statistics B-tree */
	BlockNumber first_docid_page; /* First page of docid chain for crash
								   * recovery */

	/* Hierarchical segment storage (LSM-style) */
	BlockNumber level_heads[TP_MAX_LEVELS]; /* Head of segment chain per level
											 */
	uint16 level_counts[TP_MAX_LEVELS];		/* Segment count per level */

	/*
	 * V7 field registry (Phase 6.1 multi-column).
	 *
	 * num_fields == 1 with field_names[0] == "" means "legacy single-
	 * column index" — terms are stored untagged in the dictionary and
	 * queries are unqualified. num_fields >= 2 enables tagging: every
	 * term indexed from column i is stored as `<tag_byte><stem>` where
	 * tag_byte = TP_FIELD_TAG_BASE + i. Field-scoped queries
	 * (`title:foo`) look up the tag from field_names; unqualified
	 * queries in multi-col indexes expand to an OR over tagged versions.
	 */
	uint16 num_fields;
	uint16 _pad_v7;
	char   field_names[TP_MAX_FIELDS][TP_MAX_FIELD_NAME_LEN + 1];

	/*
	 * V8 per-field BM25 weight multipliers (Phase 6.1b).
	 *
	 * At scoring time, each resolved term's IDF is multiplied by
	 * field_weights[field_idx]. 0.0 means "use default 1.0" so
	 * zero-initialized legacy V7 structs are read as uniform 1.0
	 * weights — backward-compatible on V7→V8 metapage upgrades.
	 * Value is stored exactly as the user specified; no clamping.
	 */
	float4 field_weights[TP_MAX_FIELDS];

	/*
	 * Per-field length totals (BM25F).
	 *
	 * Σ tokens contributed by column f across all docs.  Updated at
	 * spill (memtable → segment) and at end-of-build.  Per-field
	 * avgdl_f = total_len_per_field[f] / total_docs.  Single-col
	 * indexes leave entries other than [0] zero; the accessor below
	 * falls back to the corpus-wide avgdl in that case.
	 */
	uint64 total_len_per_field[TP_MAX_FIELDS];

	/*
	 * Per-field content format (plain / html / markdown).
	 * 0 = TP_FORMAT_PLAIN (default), so zero-initialized legacy
	 * metapages behave identically to explicit plain.
	 */
	uint8 field_formats[TP_MAX_FIELDS];
} TpIndexMetaPageData;

typedef TpIndexMetaPageData *TpIndexMetaPage;

/*
 * Document ID page structure for crash recovery
 * Stores ItemPointerData (ctid) entries for documents in the index.
 * Magic and version constants are defined in constants.h.
 */
typedef struct TpDocidPageHeader
{
	uint32		magic;		/* TP_DOCID_PAGE_MAGIC */
	uint32		version;	/* TP_DOCID_PAGE_VERSION */
	uint32		num_docids; /* Number of docids stored on this page */
	BlockNumber next_page;	/* Next page in chain, or InvalidBlockNumber */
} TpDocidPageHeader;

/*
 * Metapage operations
 *
 * tp_init_metapage_multi initializes with explicit field registry.
 * tp_init_metapage is the legacy single-column wrapper (field_names
 * empty, num_fields=1) — kept for any callers that don't yet know
 * about multi-col.
 */
extern void tp_init_metapage_multi(
		Page			   page,
		Oid				   text_config_oid,
		int				   num_fields,
		const char *const *field_names,
		const float4	  *field_weights,
		const uint8		  *field_formats);

extern void			   tp_init_metapage(Page page, Oid text_config_oid);
extern TpIndexMetaPage tp_get_metapage(Relation index);

/*
 * Field registry accessors. Return the field name at index i, or
 * NULL when i is out of range. tp_metapage_find_field returns the
 * 0-based index of the named field, or -1 when not found.
 */
static inline bool
tp_metapage_is_multi_col(const TpIndexMetaPageData *metap)
{
	return metap->num_fields > 1;
}

static inline const char *
tp_metapage_field_name(const TpIndexMetaPageData *metap, int idx)
{
	if (idx < 0 || idx >= metap->num_fields)
		return NULL;
	return metap->field_names[idx];
}

/*
 * Return the BM25 multiplier for field `idx`. Treats 0.0 as "unset"
 * and returns 1.0 — both for genuinely-unspecified fields and for
 * any zero-initialized tail in a legacy metapage.
 */
static inline float4
tp_metapage_field_weight(const TpIndexMetaPageData *metap, int idx)
{
	float4 w;
	if (idx < 0 || idx >= metap->num_fields)
		return 1.0f;
	w = metap->field_weights[idx];
	return (w > 0.0f) ? w : 1.0f;
}

/*
 * Per-field average document length for BM25F.  Falls back to
 * fallback_avgdl when the per-field total is zero (legacy index, or
 * single-col where only [0] is meaningful and equals total_len/total_docs).
 */
static inline float4
tp_metapage_field_avgdl(
		const TpIndexMetaPageData *metap, int idx, float4 fallback_avgdl)
{
	uint64 per_field_total;
	if (idx < 0 || idx >= metap->num_fields)
		return fallback_avgdl;
	per_field_total = metap->total_len_per_field[idx];
	if (per_field_total == 0 || metap->total_docs == 0)
		return fallback_avgdl;
	return (float4)((double)per_field_total / (double)metap->total_docs);
}

static inline uint8
tp_metapage_field_format(const TpIndexMetaPageData *metap, int idx)
{
	if (idx < 0 || idx >= metap->num_fields)
		return 0; /* TP_FORMAT_PLAIN */
	return metap->field_formats[idx];
}

extern int
tp_metapage_find_field(const TpIndexMetaPageData *metap, const char *name);

/*
 * Persist the shared-memory atomic into the metapage.  Called at
 * every spill site; caller must hold LW_EXCLUSIVE on the per-index
 * lock.
 */
extern void
tp_sync_metapage_stats(Relation index, TpLocalIndexState *index_state);
/*
 * Document ID operations for crash recovery
 */
extern void tp_add_docid_to_pages(Relation index, ItemPointer ctid);
extern void tp_clear_docid_pages(Relation index);
extern void tp_invalidate_docid_cache(void);
