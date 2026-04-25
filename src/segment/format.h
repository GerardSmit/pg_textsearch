/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * format.h - On-disk format definitions for segment storage
 */
#pragma once

#include <postgres.h>

#include <storage/itemptr.h>
#include <utils/timestamp.h>

/*
 * Page types for segment pages.
 * Magic and version constants are defined in constants.h.
 */
typedef enum TpPageType
{
	TP_PAGE_SEGMENT_HEADER = 1, /* Segment header page */
	TP_PAGE_FILE_INDEX,			/* File index (page map) pages */
	TP_PAGE_DATA				/* Data pages containing actual content */
} TpPageType;

/*
 * Special area for page index pages (goes in page special area).
 * Contains magic/version for validation.
 */
typedef struct TpPageIndexSpecial
{
	uint32		magic;	   /* TP_PAGE_INDEX_MAGIC */
	uint16		version;   /* TP_PAGE_INDEX_VERSION */
	uint16		page_type; /* TpPageType enum value */
	BlockNumber next_page; /* Next page in chain, InvalidBlockNumber if last */
	uint16		num_entries; /* Number of BlockNumber entries on this page */
	uint16		flags;		 /* Reserved for future use */
} TpPageIndexSpecial;

/*
 * Segment format version.  Clean-slate format; pre-2.0 segments are
 * not readable and require REINDEX.  Version 2 adds fuzzy-term metadata.
 * Position data is encoded as varint-delta (see TpPositionsIndexEntry below).
 */
#define TP_SEGMENT_FORMAT_VERSION 2

typedef struct TpSegmentHeader
{
	uint32		magic;
	uint32		version;
	TimestampTz created_at;
	uint32		num_pages;
	uint64		data_size;

	uint32		level;
	BlockNumber next_segment;

	uint64 dictionary_offset;
	uint64 strings_offset;
	uint64 entries_offset;
	uint64 postings_offset;

	uint64 skip_index_offset;
	uint64 fieldnorm_offset;
	uint64 ctid_pages_offset;
	uint64 ctid_offsets_offset;

	uint64 alive_bitset_offset;
	uint32 alive_count;

	uint32 num_terms;
	uint32 num_docs;
	uint64 total_tokens;

	BlockNumber page_index;

	uint64 positions_offset; /* 0 when no positions section */
	uint64 positions_size;	 /* Bytes in the positions section */

	uint64 fuzzy_index_offset; /* 0 when no fuzzy metadata section */
	uint64 fuzzy_index_size;   /* Bytes in the fuzzy metadata section */
} TpSegmentHeader;

typedef struct TpFuzzyTermMeta
{
	uint32 lexical_chars; /* term length excluding field tag, in UTF-8 chars */
	uint8  field_tag; /* 0 for untagged, otherwise TP_FIELD_TAG_BASE + idx */
	uint8  reserved[3];
} TpFuzzyTermMeta;

/*
 * Dictionary structure for fast term lookup
 *
 * The dictionary is a sorted array of string offsets, enabling binary search.
 * Each string is stored as:
 * [length:uint32][text:char*][dict_entry_offset:uint32] This allows us to
 * quickly find a term and jump to its TpDictEntry.
 */
typedef struct TpDictionary
{
	uint32 num_terms; /* Number of terms in dictionary */
	uint32 string_offsets[FLEXIBLE_ARRAY_MEMBER]; /* Sorted array of offsets to
													 strings */
} TpDictionary;

/*
 * String entry in string pool
 *
 * TODO: Optimize storage for short strings. Current overhead is 8 bytes per
 * term (4-byte length + 4-byte dict_entry_offset). Since most stemmed terms
 * are short (3-10 chars), this overhead is significant. Options:
 * - Use 1-byte length (terms rarely > 255 chars), saves 3 bytes/term
 * - Eliminate dict_entry_offset by computing from term index, saves 4
 *   bytes/term
 * - Use varint encoding for length
 */
typedef struct TpStringEntry
{
	uint32 length;						/* String length */
	char   text[FLEXIBLE_ARRAY_MEMBER]; /* String text (variable length) */
	/* Immediately after text: uint32 dict_entry_offset */
} TpStringEntry;

/*
 * Dictionary entry - 16 bytes, block-based storage.
 *
 * Points to a skip index containing block_count TpSkipEntry
 * structures, each describing a block of up to TP_BLOCK_SIZE
 * postings.
 */
typedef struct TpDictEntry
{
	uint64 skip_index_offset; /* Offset to TpSkipEntry array for this term */
	uint32 block_count;		  /* Number of blocks (and skip entries) */
	uint32 doc_freq;		  /* Document frequency for IDF */
} __attribute__((aligned(8))) TpDictEntry;

/*
 * Per-term entry in the positions index.  The positions section
 * starts with a TpPositionsIndexEntry[num_terms] array followed by
 * the concatenated per-term position streams.  Each stream is
 * varint-delta: first position as a varint, subsequent positions as
 * varint deltas (always >= 1, tsvector positions are strictly
 * monotonic within a doc).  A reader walks postings; for each
 * posting with frequency F, reads F varints advancing a byte cursor.
 *
 * positions_byte_offset is relative to the start of the positions
 * data area (positions_offset + sizeof(TpPositionsIndexEntry) *
 * num_terms).  positions_byte_length encodes:
 *
 *   bits 0..62 — actual byte length for this term's data
 *   bit 63     — encoding flag (always 1 in this format).  Kept as a
 *                sanity bit so corrupted streams that read length 0
 *                or all-bits-set are still detectable.
 */
#define TP_POS_ENCODING_FLAG_BIT ((uint64)1 << 63)
#define TP_POS_ENCODING_MASK	 ((uint64)1 << 63)
#define TP_POS_LENGTH_MASK		 ((uint64)(~TP_POS_ENCODING_MASK))

static inline bool
tp_pos_is_varint(uint64 byte_length_raw)
{
	return (byte_length_raw & TP_POS_ENCODING_FLAG_BIT) != 0;
}

static inline uint64
tp_pos_length(uint64 byte_length_raw)
{
	return byte_length_raw & TP_POS_LENGTH_MASK;
}

static inline uint64
tp_pos_encode_length(uint64 actual_length, bool is_varint)
{
	return (actual_length & TP_POS_LENGTH_MASK) |
		   (is_varint ? TP_POS_ENCODING_FLAG_BIT : 0);
}

typedef struct TpPositionsIndexEntry
{
	uint64 positions_byte_offset; /* relative to positions data start */
	uint64 positions_byte_length; /* low 63 bits: length; high bit: varint flag
								   */
} TpPositionsIndexEntry;

/*
 * Block storage constants
 */
#define TP_BLOCK_SIZE 128 /* Documents per block (matches Tantivy) */

/*
 * Skip index entry - 20 bytes per block.
 *
 * Stored separately from posting data for cache efficiency during BMW.
 * The skip index is a dense array of these entries, one per block.
 */
typedef struct TpSkipEntry
{
	uint32 last_doc_id;	   /* Last segment-local doc ID in block */
	uint8  doc_count;	   /* Number of docs in block (1-128) */
	uint16 block_max_tf;   /* Max term frequency in block (for BMW) */
	uint8  block_max_norm; /* Min fieldnorm in block (shortest doc, for BMW) */
	uint64 posting_offset; /* Byte offset from segment start to block data */
	uint8  flags;		   /* Compression type, etc. */
	uint8  reserved[3];	   /* Future use */
} __attribute__((packed)) TpSkipEntry;

/* Skip entry flags */
#define TP_BLOCK_FLAG_UNCOMPRESSED 0x00 /* Raw doc IDs and frequencies */
#define TP_BLOCK_FLAG_DELTA		   0x01 /* Delta-encoded doc IDs */
#define TP_BLOCK_FLAG_FOR		   0x02 /* Frame-of-reference (Phase 3) */
#define TP_BLOCK_FLAG_PFOR		   0x03 /* Patched FOR (Phase 3) */

/*
 * Block posting entry - 8 bytes, used in uncompressed blocks
 *
 * Unlike TpSegmentPosting, uses segment-local doc ID instead of CTID.
 * CTID lookup happens via the doc ID -> CTID mapping table.
 *
 * Fieldnorm is stored inline (using former padding bytes) to avoid
 * per-posting buffer manager lookups during scoring. This is critical
 * for performance: each buffer manager access adds ~300-500ns even when
 * pages are cached, which dominates query time for large posting lists.
 */
typedef struct TpBlockPosting
{
	uint32 doc_id;	  /* Segment-local document ID */
	uint16 frequency; /* Term frequency in document */
	uint8  fieldnorm; /* Quantized document length (Lucene SmallFloat) */
	uint8  reserved;  /* Padding for alignment */
} TpBlockPosting;

/*
 * CTID map entry - 6 bytes per document
 *
 * Maps segment-local doc IDs to heap CTIDs. This enables using
 * compact 4-byte doc IDs in posting lists while still being able
 * to look up the actual heap tuple.
 */
typedef struct TpCtidMapEntry
{
	ItemPointerData ctid; /* 6 bytes - heap tuple location */
} __attribute__((packed)) TpCtidMapEntry;
