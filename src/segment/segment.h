/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * segment.h - Disk-based segment structures
 */
#pragma once

#include <postgres.h>

#include <access/htup_details.h>
#include <port/atomics.h>
#include <storage/buffile.h>

#include "constants.h"
#include "segment/format.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"
#include "types/fuzzy.h"
#include "utils/timestamp.h"

/*
 * BufFile segment size (1 GB).  BufFile stores data across
 * multiple 1 GB physical files.  Composite offsets encode both
 * fileno and position: fileno * TP_BUFFILE_SEG_SIZE + file_offset.
 */
#define TP_BUFFILE_SEG_SIZE ((uint64)(1024 * 1024 * 1024))

static inline uint64
tp_buffile_composite_offset(int fileno, off_t file_offset)
{
	return (uint64)fileno * TP_BUFFILE_SEG_SIZE + (uint64)file_offset;
}

static inline void
tp_buffile_decompose_offset(uint64 composite, int *fileno, off_t *offset)
{
	*fileno = (int)(composite / TP_BUFFILE_SEG_SIZE);
	*offset = (off_t)(composite % TP_BUFFILE_SEG_SIZE);
}

/*
 * Segment posting - output format for iteration
 * 14 bytes, packed. Used when converting block postings for scoring.
 *
 * doc_id is segment-local, used for deferred CTID resolution in lazy loading.
 * When CTIDs are pre-loaded, ctid is set immediately. When lazy loading,
 * ctid is invalid and doc_id is used to look up CTID at result extraction.
 */
typedef struct TpSegmentPosting
{
	ItemPointerData ctid;	   /* 6 bytes - heap tuple ID (may be invalid) */
	uint32			doc_id;	   /* 4 bytes - segment-local doc ID */
	uint16			frequency; /* 2 bytes - term frequency */
	uint16 doc_length;		   /* 2 bytes - document length (from fieldnorm) */
} __attribute__((packed)) TpSegmentPosting;

/*
 * Struct sizes for the on-disk dictionary and skip index.  Kept as
 * functions (rather than direct sizeof) so callers can pass `version`
 * for documentation; the value never depends on it after the 2.0
 * clean-slate format.
 */
static inline size_t
tp_dict_entry_size(uint32 version)
{
	(void)version;
	return sizeof(TpDictEntry);
}

static inline size_t
tp_skip_entry_size(uint32 version)
{
	(void)version;
	return sizeof(TpSkipEntry);
}

/*
 * Document length - 12 bytes (padded to 16)
 */
typedef struct TpDocLength
{
	ItemPointerData ctid;	  /* 6 bytes */
	uint16			length;	  /* 2 bytes - total terms in doc */
	uint32			reserved; /* 4 bytes padding */
} TpDocLength;

/* Look up doc_freq for a term from segments (for operator scoring) */
extern uint32 tp_segment_get_doc_freq(
		Relation index, BlockNumber first_segment, const char *term);

/* Batch lookup doc_freq for multiple terms - opens each segment once */
extern void tp_batch_get_segment_doc_freq(
		Relation	index,
		BlockNumber first_segment,
		char	  **terms,
		int			term_count,
		uint32	   *doc_freqs);

/*
 * Walk a segment chain and collect terms whose dictionary string
 * starts with 'prefix'. Each segment dictionary is sorted, so we
 * binary-search the lower bound and forward-scan while the prefix
 * matches. Output terms are palloc'd C strings in the current memory
 * context. The caller owns *terms and each individual string.
 *
 * The caller is responsible for sorting and de-duplicating across
 * segments — this function emits raw matches in segment order.
 */
extern void tp_segment_collect_prefix_terms(
		Relation	index,
		BlockNumber first_segment,
		const char *prefix,
		int			max_terms,
		char	 ***out_terms,
		int		   *out_count);

extern void tp_segment_collect_fuzzy_terms(
		Relation		   index,
		BlockNumber		   first_segment,
		const char		  *query_term,
		int				   max_distance,
		int				   max_terms,
		bool			   prefix,
		TpFuzzyCandidate **out_candidates,
		int				  *out_count);

/*
 * V6 positions lookup. Walk the segment chain for (term, ctid); if
 * found in a V6 segment that carries positions, return the ordinal
 * list for that term at that doc. Returns the number of positions
 * (== term frequency in that doc), or 0 when:
 *   - none of the segments are V6-with-positions
 *   - the term is absent from all reached segments
 *   - the ctid is absent from every posting list carrying positions
 *
 * *out_positions is palloc'd in the current context; caller pfrees.
 * Set to NULL on zero return.
 *
 * Implementation: linear walk through each segment's posting list
 * for the term, accumulating frequencies to compute the byte offset
 * into the positions section. Skip entries speed block traversal;
 * inside the matching block we scan postings linearly.
 */
extern uint32 tp_segment_read_positions_for_ctid(
		Relation		index,
		BlockNumber		first_segment,
		const char	   *term,
		ItemPointerData target_ctid,
		uint32		  **out_positions);

/*
 * Batch V6 positions lookup. Walk the segment chain + each term's
 * posting list ONCE; for each posting whose ctid is in sorted_targets[],
 * record positions at the matching index of out_positions[] / out_counts[].
 *
 * This is the hot path for phrase verification — a single
 * posting-list walk collects positions for ALL candidate docs of ALL
 * phrase tokens, reducing buffer traversal by ~num_candidates×
 * compared to calling tp_segment_read_positions_for_ctid in a loop.
 *
 * sorted_targets: ItemPointerData[] sorted ascending.
 * out_positions[i]: palloc'd on match (caller pfrees); NULL on miss.
 * out_counts[i]:    set to frequency on match, 0 on miss.
 *
 * Caller pre-initializes out_positions / out_counts to NULL / 0 and
 * can accumulate across segments and terms.
 */
extern void tp_segment_collect_positions_batch(
		Relation			   index,
		BlockNumber			   first_segment,
		const char			  *term,
		const ItemPointerData *sorted_targets,
		int					   num_targets,
		uint32				 **out_positions,
		uint32				  *out_counts);

/*
 * Merge helpers for V6 positions. TpSegmentReader is defined in
 * segment/io.h; callers include io.h before using these helpers.
 */
struct TpSegmentReader;

extern bool tp_segment_read_positions_index_entry(
		struct TpSegmentReader *reader,
		uint32					dict_idx,
		TpPositionsIndexEntry  *out);

extern void tp_segment_read_positions_raw(
		struct TpSegmentReader *reader,
		uint64					positions_data_start,
		uint64					byte_offset,
		uint32					byte_length,
		void				   *out_buf);

extern uint32
tp_segment_find_dict_idx(struct TpSegmentReader *reader, const char *term);
