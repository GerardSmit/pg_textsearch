/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * compression.h - Block compression for posting lists
 *
 * Implements delta encoding + bitpacking for posting list compression.
 * Doc IDs are delta-encoded (storing gaps instead of absolute values),
 * then both gaps and frequencies are bitpacked using the minimum bits needed.
 */
#pragma once

#include <postgres.h>

#include "segment/segment.h"

/*
 * Compressed block header - stored at start of compressed block data.
 * Total: 2 bytes header + variable packed data + 128 bytes fieldnorms
 */
typedef struct TpCompressedBlockHeader
{
	uint8 doc_id_bits; /* Bits per doc ID delta (1-32) */
	uint8 freq_bits;   /* Bits per frequency (1-16) */
} TpCompressedBlockHeader;

/*
 * Maximum compressed block size (for buffer allocation).
 * Header (2) + max doc_id bits (32*128/8=512) + max freq bits (16*128/8=256)
 * + fieldnorms (128) = 898 bytes
 */
#define TP_MAX_COMPRESSED_BLOCK_SIZE 898

/* Verify buffer size is sufficient for worst case */
StaticAssertDecl(
		TP_MAX_COMPRESSED_BLOCK_SIZE >=
				sizeof(TpCompressedBlockHeader) +
						(TP_BLOCK_SIZE * 32 + 7) /
								8 + /* max doc_id bits (32 per value) */
						(TP_BLOCK_SIZE * 16 + 7) /
								8 +	   /* max freq bits (16 per value) */
						TP_BLOCK_SIZE, /* fieldnorms (1 byte each) */
		"TP_MAX_COMPRESSED_BLOCK_SIZE too small for worst-case compression");

/*
 * Compression functions
 */

/*
 * Compute minimum bits needed to represent max_value.
 * Returns 1-32 for doc IDs, 1-16 for frequencies.
 */
extern uint8 tp_compute_bit_width(uint32 max_value);

/*
 * Compress a block of postings.
 *
 * Input: array of TpBlockPosting (uncompressed)
 * Output: compressed data written to out_buf
 * Returns: number of bytes written to out_buf
 *
 * Format:
 *   [2 bytes: TpCompressedBlockHeader]
 *   [ceil(count * doc_id_bits / 8) bytes: bitpacked doc ID deltas]
 *   [ceil(count * freq_bits / 8) bytes: bitpacked frequencies]
 *   [count bytes: fieldnorms (uncompressed)]
 */
extern uint32
tp_compress_block(TpBlockPosting *postings, uint32 count, uint8 *out_buf);

/*
 * Decompress a block of postings.
 *
 * Input: compressed data from segment
 * Output: array of TpBlockPosting (caller-allocated, size count)
 *
 * first_doc_id: The first absolute doc ID for this block (from skip entry
 *               or previous block's last_doc_id + 1). For the first block
 *               of a term, this is 0.
 */
extern void tp_decompress_block(
		const uint8	   *compressed,
		uint32			count,
		uint32			first_doc_id,
		TpBlockPosting *out_postings);

/*
 * Get the size of compressed data (for validation/debugging).
 * Parses header to compute actual size without decompressing.
 */
extern uint32 tp_compressed_block_size(const uint8 *compressed, uint32 count);

/*
 * V7 positions: LEB128-style varint-delta encoding.
 *
 * Positions within a doc are strictly monotonic 1-based ordinals
 * (tsvector guarantees this). We encode the first position as a
 * varint and each subsequent position as (cur - prev) varint. Most
 * deltas are 1-3 → 1 byte each; vs 4 bytes/position raw this is a
 * 3-4× shrink for typical English text.
 *
 * tp_positions_max_encoded_bytes(count): worst-case encoded size
 *   (5 bytes per uint32). Use for buffer sizing.
 *
 * tp_positions_encode_varint_delta: encodes count positions from
 *   positions[] into out[]. Returns bytes written.
 *
 * tp_positions_decode_varint_delta: reads exactly count positions
 *   from in[0..in_len-1] into out[]. Returns bytes consumed.
 *   Raises ERROR if the stream is truncated, contains overlong
 *   varints, or exceeds in_len.
 */
extern uint32 tp_positions_max_encoded_bytes(uint32 count);
extern uint32 tp_positions_encode_varint_delta(
		const uint32 *positions, uint32 count, uint8 *out);
extern uint32 tp_positions_decode_varint_delta(
		const uint8 *in, uint32 in_len, uint32 count, uint32 *out);
