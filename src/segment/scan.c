/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * scan.c - Zero-copy scan execution for segments
 */
#include <postgres.h>

#include <utils/memutils.h>

#include "segment/compression.h"
#include "segment/dictionary.h"
#include "segment/fieldnorm.h"
#include "segment/io.h"
#include "segment/segment.h"

/*
 * Read a skip entry by block index.
 * Used by BMW scoring to pre-compute block max scores.
 */
void
tp_segment_read_skip_entry(
		TpSegmentReader *reader,
		uint64			 skip_index_offset,
		uint32			 block_idx,
		TpSkipEntry		*skip)
{
	uint64 skip_offset = skip_index_offset +
						 (uint64)block_idx * sizeof(TpSkipEntry);

	tp_segment_read(reader, skip_offset, skip, sizeof(TpSkipEntry));
}

/*
 * Initialize iterator for a specific term in a segment.
 * Returns true if term found, false otherwise.
 */
bool
tp_segment_posting_iterator_init(
		TpSegmentPostingIterator *iter,
		TpSegmentReader			 *reader,
		const char				 *term)
{
	TpSegmentHeader *header;
	TpDictionary	 dict_header;
	int				 left, right, mid;
	char			*term_buffer = NULL;
	uint32			 buffer_size = 0;

	if (!reader || !reader->header)
		return false;

	header = reader->header;

	iter->reader		   = reader;
	iter->term			   = term;
	iter->current_block	   = 0;
	iter->current_in_block = 0;
	iter->initialized	   = false;
	iter->finished		   = true;
	iter->block_postings   = NULL;
	iter->has_block_access = false;
	memset(&iter->block_access, 0, sizeof(iter->block_access));
	iter->fallback_block	   = NULL;
	iter->fallback_block_size  = 0;
	iter->cached_skip_entries  = NULL;
	iter->compressed_buf_cache = NULL;

	if (header->num_terms == 0 || header->dictionary_offset == 0)
		return false;

	/* Read dictionary header */
	tp_segment_read(
			reader,
			header->dictionary_offset,
			&dict_header,
			sizeof(dict_header.num_terms));

	/* Binary search for the term */
	left  = 0;
	right = dict_header.num_terms - 1;

	while (left <= right)
	{
		TpStringEntry string_entry;
		int			  cmp;
		uint32		  string_offset_value;
		uint32		  string_offset;

		mid = left + (right - left) / 2;

		/* Read string offset */
		tp_segment_read(
				reader,
				header->dictionary_offset + sizeof(dict_header.num_terms) +
						(mid * sizeof(uint32)),
				&string_offset_value,
				sizeof(uint32));

		string_offset = header->strings_offset + string_offset_value;

		/* Read string length */
		tp_segment_read(
				reader, string_offset, &string_entry.length, sizeof(uint32));

		if (string_entry.length > TP_MAX_TERM_LENGTH)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("corrupt segment: term length %u exceeds "
							"maximum",
							string_entry.length)));

		/* Reallocate buffer if needed */
		if (string_entry.length + 1 > buffer_size)
		{
			if (term_buffer)
				pfree(term_buffer);
			buffer_size = string_entry.length + 1;
			term_buffer = palloc(buffer_size);
		}

		/* Read term text */
		tp_segment_read(
				reader,
				string_offset + sizeof(uint32),
				term_buffer,
				string_entry.length);
		term_buffer[string_entry.length] = '\0';

		/* Compare terms */
		cmp = strcmp(term, term_buffer);

		if (cmp == 0)
		{
			/* Found! Read dictionary entry (version-aware) */
			tp_segment_read_dict_entry(reader, header, mid, &iter->dict_entry);

			iter->dict_entry_idx = mid;
			iter->initialized	 = true;
			iter->finished		 = (iter->dict_entry.block_count == 0);

			pfree(term_buffer);
			return true;
		}
		else if (cmp < 0)
		{
			right = mid - 1;
		}
		else
		{
			left = mid + 1;
		}
	}

	pfree(term_buffer);
	return false;
}

/*
 * Load a block's postings for iteration.
 * Uses zero-copy access when block data fits within a single page and is
 * uncompressed. Compressed blocks are always decompressed into the fallback
 * buffer. CTIDs are looked up from segment-level cached arrays during
 * iteration.
 */
bool
tp_segment_posting_iterator_load_block(TpSegmentPostingIterator *iter)
{
	uint32 block_size;
	uint32 block_bytes;

	if (iter->current_block >= iter->dict_entry.block_count)
		return false;

	/* Release previous block access if any */
	if (iter->has_block_access)
	{
		tp_segment_release_direct(&iter->block_access);
		iter->has_block_access = false;
		iter->block_postings   = NULL;
	}

	/* Read skip entry: use cache if available, else read from disk */
	if (iter->cached_skip_entries)
		iter->skip_entry = iter->cached_skip_entries[iter->current_block];
	else
		tp_segment_read_skip_entry(
				iter->reader,
				iter->dict_entry.skip_index_offset,
				iter->current_block,
				&iter->skip_entry);

	block_size	= iter->skip_entry.doc_count;
	block_bytes = block_size * sizeof(TpBlockPosting);

	/* Handle compressed blocks */
	if (iter->skip_entry.flags == TP_BLOCK_FLAG_DELTA)
	{
		uint8 *compressed_buf;
		bool   free_compressed = false;

		/* Use cached buffer if available, else palloc */
		if (iter->compressed_buf_cache)
			compressed_buf = iter->compressed_buf_cache;
		else
		{
			compressed_buf	= palloc(TP_MAX_COMPRESSED_BLOCK_SIZE);
			free_compressed = true;
		}

		tp_segment_read(
				iter->reader,
				iter->skip_entry.posting_offset,
				compressed_buf,
				TP_MAX_COMPRESSED_BLOCK_SIZE);

		/* Ensure fallback buffer is large enough */
		if (block_size > iter->fallback_block_size)
		{
			if (iter->fallback_block)
				pfree(iter->fallback_block);
			iter->fallback_block	  = palloc(block_bytes);
			iter->fallback_block_size = block_size;
		}

		/* Decompress into fallback buffer */
		tp_decompress_block(
				compressed_buf, block_size, 0, iter->fallback_block);

		if (free_compressed)
			pfree(compressed_buf);
		iter->block_postings = iter->fallback_block;
	}
	else
	{
		/*
		 * Uncompressed block: try zero-copy direct access.
		 * TpBlockPosting requires 4-byte alignment (due to uint32 doc_id).
		 * If the data address is misaligned, fall back to copying.
		 */
		if (tp_segment_get_direct(
					iter->reader,
					iter->skip_entry.posting_offset,
					block_bytes,
					&iter->block_access) &&
			((uintptr_t)iter->block_access.data % sizeof(uint32)) == 0)
		{
			/* Zero-copy: point directly into the page buffer */
			iter->block_postings   = (TpBlockPosting *)iter->block_access.data;
			iter->has_block_access = true;
		}
		else
		{
			/* Release direct access if we got it but it's misaligned */
			if (iter->block_access.data != NULL)
				tp_segment_release_direct(&iter->block_access);
			/* Fallback: block spans page boundary, must copy */
			if (block_size > iter->fallback_block_size)
			{
				if (iter->fallback_block)
					pfree(iter->fallback_block);
				iter->fallback_block	  = palloc(block_bytes);
				iter->fallback_block_size = block_size;
			}

			tp_segment_read(
					iter->reader,
					iter->skip_entry.posting_offset,
					iter->fallback_block,
					block_bytes);

			iter->block_postings = iter->fallback_block;
		}
	}

	iter->current_in_block = 0;
	return true;
}

/*
 * Get next posting from iterator.
 * Converts block posting to TpSegmentPosting for scoring compatibility.
 * Returns false when no more postings.
 */
bool
tp_segment_posting_iterator_next(
		TpSegmentPostingIterator *iter, TpSegmentPosting **posting)
{
	TpBlockPosting *bp;
	uint32			doc_id;

	if (iter->finished || !iter->initialized)
		return false;

	/* Load first block if needed */
	if (iter->block_postings == NULL)
	{
		if (!tp_segment_posting_iterator_load_block(iter))
		{
			/* load_block only fails on corrupted segment data */
			if (iter->has_block_access)
			{
				tp_segment_release_direct(&iter->block_access);
				iter->has_block_access = false;
			}
			iter->finished = true;
			return false;
		}
	}

	/* Move to next block if current is exhausted */
	while (iter->current_in_block >= iter->skip_entry.doc_count)
	{
		iter->current_block++;
		if (iter->current_block >= iter->dict_entry.block_count)
		{
			/* Release block access before finishing */
			if (iter->has_block_access)
			{
				tp_segment_release_direct(&iter->block_access);
				iter->has_block_access = false;
			}
			iter->finished = true;
			return false;
		}
		if (!tp_segment_posting_iterator_load_block(iter))
		{
			/* load_block only fails on corrupted segment data */
			if (iter->has_block_access)
			{
				tp_segment_release_direct(&iter->block_access);
				iter->has_block_access = false;
			}
			iter->finished = true;
			return false;
		}
	}

	/* Get current posting from block */
	bp	   = &iter->block_postings[iter->current_in_block];
	doc_id = bp->doc_id;

	/* Always store doc_id for deferred CTID resolution */
	iter->output_posting.doc_id = doc_id;

	/*
	 * Look up CTID from segment-level cached arrays if available.
	 * By default, CTIDs are not pre-loaded and will be resolved later
	 * via tp_segment_lookup_ctid.
	 */
	if (iter->reader->cached_ctid_pages != NULL &&
		doc_id < iter->reader->cached_num_docs)
	{
		ItemPointerData tmp;
		ItemPointerSet(
				&tmp,
				iter->reader->cached_ctid_pages[doc_id],
				iter->reader->cached_ctid_offsets[doc_id]);
		memcpy(&iter->output_posting.ctid, &tmp, sizeof(ItemPointerData));
	}
	else
	{
		/* Mark CTID invalid - will be resolved later */
		ItemPointerData tmp;
		ItemPointerSetInvalid(&tmp);
		memcpy(&iter->output_posting.ctid, &tmp, sizeof(ItemPointerData));
	}

	/* Build output posting (fieldnorm is inline in bp) */
	iter->output_posting.frequency	= bp->frequency;
	iter->output_posting.doc_length = (uint16)decode_fieldnorm(bp->fieldnorm);

	*posting = &iter->output_posting;
	iter->current_in_block++;
	return true;
}

/*
 * Free iterator resources.
 */
void
tp_segment_posting_iterator_free(TpSegmentPostingIterator *iter)
{
	/* Release direct block access if active */
	if (iter->has_block_access)
	{
		tp_segment_release_direct(&iter->block_access);
		iter->has_block_access = false;
	}

	/* Free fallback buffer if allocated */
	if (iter->fallback_block)
	{
		pfree(iter->fallback_block);
		iter->fallback_block = NULL;
	}

	/*
	 * Note: cached_skip_entries and compressed_buf_cache are borrowed
	 * pointers owned by the BMW caller.  Do NOT free them here.
	 */
	iter->cached_skip_entries  = NULL;
	iter->compressed_buf_cache = NULL;
	iter->block_postings	   = NULL;
}

/*
 * Get current doc ID from iterator.
 * Returns UINT32_MAX if iterator is finished or not positioned.
 */
uint32
tp_segment_posting_iterator_current_doc_id(TpSegmentPostingIterator *iter)
{
	if (iter->finished || !iter->initialized || iter->block_postings == NULL)
		return UINT32_MAX;

	if (iter->current_in_block >= iter->skip_entry.doc_count)
		return UINT32_MAX;

	return iter->block_postings[iter->current_in_block].doc_id;
}

/*
 * Seek iterator to target doc ID or the first doc ID >= target.
 * Returns true if a posting was found, false if exhausted.
 *
 * Uses binary search on skip entries (each has last_doc_id) to find
 * the right block, then linear scan within the block. This is the
 * core operation for WAND-style doc-ID ordered traversal.
 */
bool
tp_segment_posting_iterator_seek(
		TpSegmentPostingIterator *iter,
		uint32					  target_doc_id,
		TpSegmentPosting		**posting)
{
	uint32		block_count;
	int			left, right, mid;
	uint32		target_block;
	TpSkipEntry skip;

	if (!iter->initialized || iter->finished)
		return false;

	block_count = iter->dict_entry.block_count;

	/*
	 * Binary search skip entries to find block containing target_doc_id.
	 * Each skip entry has last_doc_id = maximum doc ID in that block.
	 * We want the first block where last_doc_id >= target_doc_id.
	 */
	left  = 0;
	right = block_count - 1;

	while (left < right)
	{
		mid = left + (right - left) / 2;

		/* Read skip entry to get last_doc_id */
		tp_segment_read_skip_entry(
				iter->reader, iter->dict_entry.skip_index_offset, mid, &skip);

		if (skip.last_doc_id < target_doc_id)
		{
			/* Target is past this block */
			left = mid + 1;
		}
		else
		{
			/* Target might be in this block or earlier */
			right = mid;
		}
	}

	target_block = left;

	/* Check if target is past all blocks */
	if (target_block >= block_count)
	{
		iter->finished = true;
		return false;
	}

	/* Load the target block */
	iter->current_block	   = target_block;
	iter->current_in_block = 0;
	iter->finished		   = false;

	if (!tp_segment_posting_iterator_load_block(iter))
	{
		iter->finished = true;
		return false;
	}

	/* Linear scan within block to find target or first doc >= target */
	while (iter->current_in_block < iter->skip_entry.doc_count)
	{
		TpBlockPosting *bp = &iter->block_postings[iter->current_in_block];

		if (bp->doc_id >= target_doc_id)
		{
			/* Found it - convert to output posting */
			iter->output_posting.doc_id = bp->doc_id;

			/* Resolve CTID if cached, otherwise leave invalid for later */
			if (iter->reader->cached_ctid_pages != NULL &&
				bp->doc_id < iter->reader->cached_num_docs)
			{
				ItemPointerData tmp;
				ItemPointerSet(
						&tmp,
						iter->reader->cached_ctid_pages[bp->doc_id],
						iter->reader->cached_ctid_offsets[bp->doc_id]);
				memcpy(&iter->output_posting.ctid,
					   &tmp,
					   sizeof(ItemPointerData));
			}
			else
			{
				ItemPointerData tmp;
				ItemPointerSetInvalid(&tmp);
				memcpy(&iter->output_posting.ctid,
					   &tmp,
					   sizeof(ItemPointerData));
			}

			iter->output_posting.frequency	= bp->frequency;
			iter->output_posting.doc_length = (uint16)decode_fieldnorm(
					bp->fieldnorm);

			*posting = &iter->output_posting;
			return true;
		}

		iter->current_in_block++;
	}

	/*
	 * Exhausted this block without finding target.
	 * This shouldn't happen if last_doc_id was correct, but handle it
	 * by moving to next block and trying next().
	 */
	iter->current_block++;
	if (iter->current_block >= block_count)
	{
		iter->finished = true;
		return false;
	}

	/* Load next block and return first posting */
	iter->current_in_block = 0;
	return tp_segment_posting_iterator_next(iter, posting);
}

/*
 * Sum doc_freq for a term across all segments.
 */
uint32
tp_segment_get_doc_freq(
		Relation index, BlockNumber first_segment, const char *term)
{
	BlockNumber		 current	 = first_segment;
	TpSegmentReader *reader		 = NULL;
	uint32			 doc_freq	 = 0;
	char			*term_buffer = NULL;
	uint32			 buffer_size = 0;

	while (current != InvalidBlockNumber)
	{
		TpSegmentHeader *header;
		TpDictionary	 dict_header;
		int				 left, right;

		reader = tp_segment_open(index, current);
		if (!reader)
			break;

		header = reader->header;
		if (header->num_terms == 0 || header->dictionary_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		tp_segment_read(
				reader,
				header->dictionary_offset,
				&dict_header,
				sizeof(dict_header.num_terms));

		left  = 0;
		right = dict_header.num_terms - 1;

		while (left <= right)
		{
			TpStringEntry string_entry;
			int			  cmp;
			uint32		  string_offset_value;
			uint32		  string_offset;
			int			  mid = left + (right - left) / 2;

			tp_segment_read(
					reader,
					header->dictionary_offset + sizeof(dict_header.num_terms) +
							(mid * sizeof(uint32)),
					&string_offset_value,
					sizeof(uint32));

			string_offset = header->strings_offset + string_offset_value;

			tp_segment_read(
					reader,
					string_offset,
					&string_entry.length,
					sizeof(uint32));

			if (string_entry.length > TP_MAX_TERM_LENGTH)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("corrupt segment: term length %u "
								"exceeds maximum",
								string_entry.length)));

			if (string_entry.length + 1 > buffer_size)
			{
				if (term_buffer)
					pfree(term_buffer);
				buffer_size = string_entry.length + 1;
				term_buffer = palloc(buffer_size);
			}

			tp_segment_read(
					reader,
					string_offset + sizeof(uint32),
					term_buffer,
					string_entry.length);
			term_buffer[string_entry.length] = '\0';

			cmp = strcmp(term, term_buffer);

			if (cmp == 0)
			{
				TpDictEntry dict_entry;
				tp_segment_read_dict_entry(reader, header, mid, &dict_entry);
				doc_freq += dict_entry.doc_freq;
				break;
			}
			else if (cmp < 0)
			{
				right = mid - 1;
			}
			else
			{
				left = mid + 1;
			}
		}

		current = header->next_segment;
		tp_segment_close(reader);
	}

	if (term_buffer)
		pfree(term_buffer);

	return doc_freq;
}

/*
 * Read a dictionary entry's term text into 'buf' (resizing if needed).
 * Returns the term length. *buf must be palloc'd or NULL.
 */
static uint32
tp_read_dict_term(
		TpSegmentReader *reader,
		TpSegmentHeader *header,
		uint32			 dict_idx,
		char		   **buf,
		uint32			*buf_size)
{
	uint32		 string_offset_value;
	uint32		 string_offset;
	uint32		 string_length;
	TpDictionary dict_header;

	tp_segment_read(
			reader,
			header->dictionary_offset,
			&dict_header,
			sizeof(dict_header.num_terms));

	tp_segment_read(
			reader,
			header->dictionary_offset + sizeof(dict_header.num_terms) +
					(dict_idx * sizeof(uint32)),
			&string_offset_value,
			sizeof(uint32));

	string_offset = header->strings_offset + string_offset_value;

	tp_segment_read(reader, string_offset, &string_length, sizeof(uint32));

	if (string_length > TP_MAX_TERM_LENGTH)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupt segment: term length %u exceeds maximum",
						string_length)));

	if (string_length + 1 > *buf_size)
	{
		if (*buf)
			pfree(*buf);
		*buf_size = string_length + 1;
		*buf	  = palloc(*buf_size);
	}

	tp_segment_read(
			reader, string_offset + sizeof(uint32), *buf, string_length);
	(*buf)[string_length] = '\0';
	return string_length;
}

/*
 * Walk segments and collect dictionary terms starting with 'prefix'.
 * Each segment dictionary is sorted, so we binary-search the lower
 * bound (first index whose term >= prefix) and forward-scan while
 * matches continue. Stops when 'max_terms' have been collected.
 */
void
tp_segment_collect_prefix_terms(
		Relation	index,
		BlockNumber first_segment,
		const char *prefix,
		int			max_terms,
		char	 ***out_terms,
		int		   *out_count)
{
	BlockNumber current		= first_segment;
	char	   *term_buffer = NULL;
	uint32		buffer_size = 0;
	size_t		prefix_len;
	char	  **terms	 = NULL;
	int			count	 = 0;
	int			capacity = 0;

	*out_terms = NULL;
	*out_count = 0;

	if (prefix == NULL || *prefix == '\0' || max_terms <= 0)
		return;

	prefix_len = strlen(prefix);

	while (current != InvalidBlockNumber && count < max_terms)
	{
		TpSegmentReader *reader;
		TpSegmentHeader *header;
		TpDictionary	 dict_header;
		int				 left, right, lower_bound;
		uint32			 idx;

		reader = tp_segment_open(index, current);
		if (!reader)
			break;

		header = reader->header;
		if (header->num_terms == 0 || header->dictionary_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		tp_segment_read(
				reader,
				header->dictionary_offset,
				&dict_header,
				sizeof(dict_header.num_terms));

		/*
		 * Binary search for lower bound: first dict index whose term is
		 * >= prefix. Compare only over min(prefix_len, term_len) bytes —
		 * a term that EQUALS the prefix-as-string is still a match
		 * (zero-length suffix). For the lower bound we treat ties as
		 * "term >= prefix".
		 */
		left  = 0;
		right = (int)dict_header.num_terms; /* exclusive */
		while (left < right)
		{
			int	   mid = left + (right - left) / 2;
			uint32 term_len;
			int	   cmp;
			size_t cmp_len;

			term_len = tp_read_dict_term(
					reader, header, (uint32)mid, &term_buffer, &buffer_size);

			cmp_len = (term_len < prefix_len) ? term_len : prefix_len;
			cmp		= memcmp(term_buffer, prefix, cmp_len);
			if (cmp == 0)
			{
				/* term shorter than prefix → less than prefix */
				if (term_len < prefix_len)
					cmp = -1;
				else
					cmp = 0; /* term starts with prefix → first match */
			}

			if (cmp < 0)
				left = mid + 1;
			else
				right = mid;
		}
		lower_bound = left;

		/* Forward scan while prefix matches */
		for (idx = (uint32)lower_bound;
			 idx < dict_header.num_terms && count < max_terms;
			 idx++)
		{
			uint32 term_len;
			char  *copy;

			term_len = tp_read_dict_term(
					reader, header, idx, &term_buffer, &buffer_size);

			if (term_len < prefix_len)
				break;
			if (memcmp(term_buffer, prefix, prefix_len) != 0)
				break;

			if (count >= capacity)
			{
				int new_cap = (capacity == 0) ? 8 : capacity * 2;
				if (new_cap > max_terms)
					new_cap = max_terms;
				if (terms == NULL)
					terms = palloc(new_cap * sizeof(char *));
				else
					terms = repalloc(terms, new_cap * sizeof(char *));
				capacity = new_cap;
			}

			copy = palloc(term_len + 1);
			memcpy(copy, term_buffer, term_len + 1);
			terms[count++] = copy;
		}

		current = header->next_segment;
		tp_segment_close(reader);
	}

	if (term_buffer)
		pfree(term_buffer);

	*out_terms = terms;
	*out_count = count;
}

void
tp_segment_collect_fuzzy_terms(
		Relation		   index,
		BlockNumber		   first_segment,
		const char		  *query_term,
		int				   max_distance,
		int				   max_terms,
		bool			   prefix,
		TpFuzzyCandidate **out_candidates,
		int				  *out_count)
{
	BlockNumber		  current	  = first_segment;
	char			 *term_buffer = NULL;
	uint32			  buffer_size = 0;
	TpFuzzyCandidate *candidates  = NULL;
	int				  count		  = 0;
	int				  capacity	  = 0;
	TpFuzzyTermMeta	  query_meta;

	*out_candidates = NULL;
	*out_count		= 0;

	if (query_term == NULL || *query_term == '\0' || max_terms <= 0 ||
		max_distance < 0)
		return;

	tp_fuzzy_fill_term_meta(query_term, &query_meta);

	while (current != InvalidBlockNumber)
	{
		TpSegmentReader *reader;
		TpSegmentHeader *header;
		TpDictionary	 dict_header;
		uint32			 idx;
		bool			 has_meta = false;

		reader = tp_segment_open(index, current);
		if (!reader)
			break;

		header = reader->header;
		if (header->num_terms == 0 || header->dictionary_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		tp_segment_read(
				reader,
				header->dictionary_offset,
				&dict_header,
				sizeof(dict_header.num_terms));

		has_meta = header->fuzzy_index_offset != 0 &&
				   header->fuzzy_index_size >= (uint64)dict_header.num_terms *
													   sizeof(TpFuzzyTermMeta);

		for (idx = 0; idx < dict_header.num_terms; idx++)
		{
			uint32 term_len;
			uint8  distance;

			if (has_meta)
			{
				TpFuzzyTermMeta meta;

				tp_segment_read(
						reader,
						header->fuzzy_index_offset +
								(uint64)idx * sizeof(TpFuzzyTermMeta),
						&meta,
						sizeof(TpFuzzyTermMeta));

				if (meta.field_tag != query_meta.field_tag)
					continue;
				if (!prefix &&
					abs((int)meta.lexical_chars -
						(int)query_meta.lexical_chars) > max_distance)
					continue;
				if (prefix && meta.lexical_chars + (uint32)max_distance <
									  query_meta.lexical_chars)
					continue;
			}

			term_len = tp_read_dict_term(
					reader, header, idx, &term_buffer, &buffer_size);
			(void)term_len;

			if (tp_fuzzy_match_term(
						query_term,
						term_buffer,
						max_distance,
						prefix,
						&distance))
			{
				tp_fuzzy_candidates_insert(
						&candidates,
						&count,
						&capacity,
						pstrdup(term_buffer),
						distance,
						max_terms);
			}
		}

		current = header->next_segment;
		tp_segment_close(reader);
	}

	if (term_buffer)
		pfree(term_buffer);

	*out_candidates = candidates;
	*out_count		= count;
}

/*
 * V6 positions lookup. Given a term and a heap CTID, find the matching
 * posting in one of the segments on the chain and slice its positions
 * out of the segment's positions section. Returns the frequency (=
 * positions count); 0 when no V6-with-positions segment carries this
 * (term, ctid) pair.
 */
uint32
tp_segment_read_positions_for_ctid(
		Relation		index,
		BlockNumber		first_segment,
		const char	   *term,
		ItemPointerData target_ctid,
		uint32		  **out_positions)
{
	uint32 cnt = 0;

	*out_positions = NULL;
	/*
	 * Delegate to the batch collector. It walks the segment chain
	 * once and handles V6 raw + V7 varint encoding uniformly. This
	 * single-CTID variant is kept for API compatibility; the phrase
	 * verifier uses the batch API directly.
	 */
	tp_segment_collect_positions_batch(
			index, first_segment, term, &target_ctid, 1, out_positions, &cnt);
	return cnt;
}

#if 0  /* legacy pre-batch implementation, kept for reference */
static uint32
tp_segment_read_positions_for_ctid_legacy(
		Relation		 index,
		BlockNumber		 first_segment,
		const char		*term,
		ItemPointerData	 target_ctid,
		uint32		   **out_positions)
{
	BlockNumber		 current = first_segment;
	TpSegmentReader *reader;

	while (current != InvalidBlockNumber)
	{
		TpSegmentHeader		 *header;
		TpDictionary		  dict_header;
		int					  left, right, mid;
		bool				  found_term	  = false;
		uint32				  found_dict_idx = UINT32_MAX;
		char				 *term_buffer	  = NULL;
		uint32				  buffer_size	  = 0;
		TpPositionsIndexEntry pos_index_entry;
		uint64				  pos_data_start;
		uint32				  frequency_prefix_sum = 0;
		uint32				  matched_frequency		 = 0;
		bool				  matched				 = false;
		TpSegmentPostingIterator iter;

		/* Preload CTIDs so we can match target_ctid against doc_id */
		reader = tp_segment_open_ex(index, current, true);
		if (!reader)
			break;

		header = reader->header;

		/* Segment built without positions → skip */
		if (header->positions_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		if (header->num_terms == 0 || header->dictionary_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		/* Binary search the dict for 'term' (same pattern as existing init) */
		tp_segment_read(
				reader,
				header->dictionary_offset,
				&dict_header,
				sizeof(dict_header.num_terms));
		left  = 0;
		right = dict_header.num_terms - 1;
		while (left <= right)
		{
			TpStringEntry string_entry;
			int			  cmp;
			uint32		  string_offset_value;
			uint32		  string_offset;

			mid = left + (right - left) / 2;
			tp_segment_read(
					reader,
					header->dictionary_offset + sizeof(dict_header.num_terms) +
							(mid * sizeof(uint32)),
					&string_offset_value,
					sizeof(uint32));
			string_offset = header->strings_offset + string_offset_value;
			tp_segment_read(
					reader, string_offset, &string_entry.length, sizeof(uint32));
			if (string_entry.length > TP_MAX_TERM_LENGTH)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("corrupt segment: term length %u exceeds "
								"maximum",
								string_entry.length)));
			if (string_entry.length + 1 > buffer_size)
			{
				if (term_buffer)
					pfree(term_buffer);
				buffer_size = string_entry.length + 1;
				term_buffer = palloc(buffer_size);
			}
			tp_segment_read(
					reader,
					string_offset + sizeof(uint32),
					term_buffer,
					string_entry.length);
			term_buffer[string_entry.length] = '\0';
			cmp = strcmp(term, term_buffer);
			if (cmp == 0)
			{
				found_term	   = true;
				found_dict_idx = (uint32)mid;
				break;
			}
			else if (cmp < 0)
				right = mid - 1;
			else
				left = mid + 1;
		}

		if (term_buffer)
			pfree(term_buffer);

		if (!found_term)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		/*
		 * Single-target lookup via the batch API for encoding
		 * neutrality — handles V6 raw and V7 varint uniformly.
		 * Slightly heavier than a direct walk but callers of this
		 * single-CTID variant are rare.
		 */
		tp_segment_close(reader);
		(void)iter;
		(void)pos_index_entry;
		(void)pos_data_start;
		(void)frequency_prefix_sum;
		(void)matched_frequency;
		(void)matched;
		break;
	}

	return 0;
}
#endif /* 0 — legacy */

/*
 * Binary-search a sorted ItemPointerData array for the given key.
 * Returns the index on match, -1 on miss.
 */
static int
seg_ctid_bsearch(const ItemPointerData *arr, int n, const ItemPointerData *key)
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
tp_segment_collect_positions_batch(
		Relation			   index,
		BlockNumber			   first_segment,
		const char			  *term,
		const ItemPointerData *sorted_targets,
		int					   num_targets,
		uint32				 **out_positions,
		uint32				  *out_counts)
{
	BlockNumber current = first_segment;

	if (term == NULL || num_targets == 0)
		return;

	while (current != InvalidBlockNumber)
	{
		TpSegmentReader			*reader;
		TpSegmentHeader			*header;
		TpPositionsIndexEntry	 pos_index_entry;
		uint64					 pos_data_start;
		uint32					 dict_idx;
		TpSegmentPostingIterator iter;
		TpSegmentPosting		*posting;

		/* Need CTIDs preloaded so iterator fills posting->ctid */
		reader = tp_segment_open_ex(index, current, true);
		if (!reader)
			break;
		header = reader->header;

		if (header->positions_offset == 0 || header->num_terms == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		dict_idx = tp_segment_find_dict_idx(reader, term);
		if (dict_idx == UINT32_MAX ||
			!tp_segment_read_positions_index_entry(
					reader, dict_idx, &pos_index_entry) ||
			tp_pos_length(pos_index_entry.positions_byte_length) == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		pos_data_start = header->positions_offset +
						 (uint64)header->num_terms *
								 sizeof(TpPositionsIndexEntry) +
						 pos_index_entry.positions_byte_offset;

		if (!tp_segment_posting_iterator_init(&iter, reader, term))
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		{
			uint64 term_pos_len = tp_pos_length(
					pos_index_entry.positions_byte_length);
			bool is_varint = tp_pos_is_varint(
					pos_index_entry.positions_byte_length);
			uint8 *term_pos_buf	 = NULL;
			uint32 raw_cursor	 = 0; /* V6 byte cursor */
			uint32 varint_cursor = 0; /* V7 byte cursor into term_pos_buf */

			/*
			 * Prefetch the positions byte range for this term
			 * upfront. One prefetch call covers every read we're
			 * about to make against this term. Under cold cache
			 * this issues async I/O (capped by
			 * effective_io_concurrency) that pipelines with the
			 * posting-list walk.
			 */
			tp_segment_prefetch(
					reader,
					pos_data_start,
					(uint32)Min((uint64)UINT32_MAX, term_pos_len));

			/*
			 * For varint encoding we can't compute per-posting byte
			 * offsets without a sequential decode — pre-read the
			 * whole term's positions bytes into memory and walk
			 * cursor-style. term_pos_len is bounded by positions
			 * section size (MB range), fine for palloc.
			 */
			if (is_varint && term_pos_len > 0)
			{
				if (term_pos_len > (uint64)UINT32_MAX)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("positions section for term too large")));
				term_pos_buf = palloc((uint32)term_pos_len);
				tp_segment_read(
						reader,
						pos_data_start,
						term_pos_buf,
						(uint32)term_pos_len);
			}

			while (tp_segment_posting_iterator_next(&iter, &posting))
			{
				uint16 freq = posting->frequency;
				int	   idx;

				idx = seg_ctid_bsearch(
						sorted_targets, num_targets, &posting->ctid);

				if (is_varint)
				{
					if (freq == 0)
						continue;
					if (idx >= 0 && out_positions[idx] == NULL)
					{
						uint32 *buf = palloc(freq * sizeof(uint32));
						varint_cursor += tp_positions_decode_varint_delta(
								term_pos_buf + varint_cursor,
								(uint32)term_pos_len - varint_cursor,
								freq,
								buf);
						out_positions[idx] = buf;
						out_counts[idx]	   = (uint32)freq;
					}
					else
					{
						/* Skip this posting's varints to advance cursor */
						uint32 tmp[TP_BLOCK_SIZE];
						uint32 chunk;
						uint32 remaining = freq;
						while (remaining > 0)
						{
							chunk = Min(remaining, TP_BLOCK_SIZE);
							varint_cursor += tp_positions_decode_varint_delta(
									term_pos_buf + varint_cursor,
									(uint32)term_pos_len - varint_cursor,
									chunk,
									tmp);
							remaining -= chunk;
						}
					}
				}
				else /* raw uint32 */
				{
					if (idx >= 0 && out_positions[idx] == NULL && freq > 0)
					{
						uint32 *buf = palloc(freq * sizeof(uint32));
						tp_segment_read(
								reader,
								pos_data_start +
										(uint64)raw_cursor * sizeof(uint32),
								buf,
								freq * sizeof(uint32));
						out_positions[idx] = buf;
						out_counts[idx]	   = (uint32)freq;
					}
					raw_cursor += freq;
				}
			}

			if (term_pos_buf)
				pfree(term_pos_buf);
		}

		tp_segment_posting_iterator_free(&iter);

		current = header->next_segment;
		tp_segment_close(reader);
	}
}

uint32
tp_segment_find_dict_idx(struct TpSegmentReader *reader, const char *term)
{
	TpSegmentHeader *header;
	TpDictionary	 dict_header;
	int				 left, right, mid;
	char			*term_buffer = NULL;
	uint32			 buffer_size = 0;
	uint32			 found_idx	 = UINT32_MAX;

	if (!reader || !reader->header)
		return UINT32_MAX;
	header = reader->header;
	if (header->num_terms == 0 || header->dictionary_offset == 0)
		return UINT32_MAX;

	tp_segment_read(
			reader,
			header->dictionary_offset,
			&dict_header,
			sizeof(dict_header.num_terms));

	left  = 0;
	right = dict_header.num_terms - 1;
	while (left <= right)
	{
		uint32 string_offset_value;
		uint32 string_offset;
		uint32 string_length;
		int	   cmp;

		mid = left + (right - left) / 2;
		tp_segment_read(
				reader,
				header->dictionary_offset + sizeof(dict_header.num_terms) +
						(mid * sizeof(uint32)),
				&string_offset_value,
				sizeof(uint32));
		string_offset = header->strings_offset + string_offset_value;
		tp_segment_read(reader, string_offset, &string_length, sizeof(uint32));
		if (string_length > TP_MAX_TERM_LENGTH)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("corrupt segment: term length %u exceeds maximum",
							string_length)));
		if (string_length + 1 > buffer_size)
		{
			if (term_buffer)
				pfree(term_buffer);
			buffer_size = string_length + 1;
			term_buffer = palloc(buffer_size);
		}
		tp_segment_read(
				reader,
				string_offset + sizeof(uint32),
				term_buffer,
				string_length);
		term_buffer[string_length] = '\0';
		cmp						   = strcmp(term, term_buffer);
		if (cmp == 0)
		{
			found_idx = (uint32)mid;
			break;
		}
		else if (cmp < 0)
			right = mid - 1;
		else
			left = mid + 1;
	}
	if (term_buffer)
		pfree(term_buffer);
	return found_idx;
}

bool
tp_segment_read_positions_index_entry(
		struct TpSegmentReader *reader,
		uint32					dict_idx,
		TpPositionsIndexEntry  *out)
{
	TpSegmentHeader *header;

	if (!reader || !reader->header)
		return false;
	header = reader->header;
	if (header->positions_offset == 0)
		return false;
	if (dict_idx >= header->num_terms)
		return false;

	tp_segment_read(
			reader,
			header->positions_offset +
					(uint64)dict_idx * sizeof(TpPositionsIndexEntry),
			out,
			sizeof(TpPositionsIndexEntry));
	return true;
}

void
tp_segment_read_positions_raw(
		struct TpSegmentReader *reader,
		uint64					positions_data_start,
		uint64					byte_offset,
		uint32					byte_length,
		void				   *out_buf)
{
	if (byte_length == 0)
		return;
	tp_segment_read(
			reader, positions_data_start + byte_offset, out_buf, byte_length);
}

/*
 * Batch lookup doc_freq for multiple terms across a segment chain.
 * Opens each segment ONCE and looks up all terms, avoiding
 * O(terms * segments) segment opens.
 *
 * doc_freqs array should be pre-initialized (typically to 0 or memtable
 * counts). This function ADDS segment doc_freqs to existing values.
 */
void
tp_batch_get_segment_doc_freq(
		Relation	index,
		BlockNumber first_segment,
		char	  **terms,
		int			term_count,
		uint32	   *doc_freqs)
{
	BlockNumber current		= first_segment;
	char	   *term_buffer = NULL;
	uint32		buffer_size = 0;

	while (current != InvalidBlockNumber)
	{
		TpSegmentReader *reader;
		TpSegmentHeader *header;
		TpDictionary	 dict_header;
		int				 term_idx;

		/* Open segment ONCE for all terms */
		reader = tp_segment_open(index, current);
		if (!reader)
			break;

		header = reader->header;

		if (header->num_terms == 0 || header->dictionary_offset == 0)
		{
			current = header->next_segment;
			tp_segment_close(reader);
			continue;
		}

		/* Read dictionary header once per segment */
		tp_segment_read(
				reader,
				header->dictionary_offset,
				&dict_header,
				sizeof(dict_header.num_terms));

		/* Look up each term in this segment */
		for (term_idx = 0; term_idx < term_count; term_idx++)
		{
			const char *term  = terms[term_idx];
			int			left  = 0;
			int			right = dict_header.num_terms - 1;

			/* Binary search for term in dictionary */
			while (left <= right)
			{
				int	   mid = left + (right - left) / 2;
				uint32 string_offset_value;
				uint32 string_offset;
				uint32 string_length;
				int	   cmp;

				tp_segment_read(
						reader,
						header->dictionary_offset +
								sizeof(dict_header.num_terms) +
								(mid * sizeof(uint32)),
						&string_offset_value,
						sizeof(uint32));

				string_offset = header->strings_offset + string_offset_value;

				tp_segment_read(
						reader, string_offset, &string_length, sizeof(uint32));

				if (string_length > TP_MAX_TERM_LENGTH)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("corrupt segment: term length %u "
									"exceeds maximum",
									string_length)));

				if (string_length + 1 > buffer_size)
				{
					if (term_buffer)
						pfree(term_buffer);
					buffer_size = string_length + 1;
					term_buffer = palloc(buffer_size);
				}

				tp_segment_read(
						reader,
						string_offset + sizeof(uint32),
						term_buffer,
						string_length);
				term_buffer[string_length] = '\0';

				cmp = strcmp(term, term_buffer);

				if (cmp == 0)
				{
					/* Found - read dict entry and add doc_freq */
					TpDictEntry dict_entry;
					tp_segment_read_dict_entry(
							reader, header, mid, &dict_entry);
					doc_freqs[term_idx] += dict_entry.doc_freq;
					break;
				}
				else if (cmp < 0)
				{
					right = mid - 1;
				}
				else
				{
					left = mid + 1;
				}
			}
		}

		/* Move to next segment and close this one */
		current = header->next_segment;
		tp_segment_close(reader);
	}

	if (term_buffer)
		pfree(term_buffer);
}
