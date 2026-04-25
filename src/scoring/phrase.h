/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * phrase.h - Phrase / phrase-prefix positional verifier
 *
 * Segments store per-term position ordinals; the fast path reads
 * them directly.  When positions are unavailable (pre-positions
 * segments or memtable), falls back to re-tokenizing the candidate
 * document via to_tsvector_byid and scanning for ordered adjacency.
 */
#pragma once

#include <postgres.h>

#include <utils/rel.h>

/*
 * Result codes from tp_phrase_verify_text(). Negative values
 * indicate "definitely no match"; positive indicate "match"; zero
 * indicates "indeterminate" (e.g., empty doc) — caller treats as
 * non-match.
 */
typedef enum
{
	TP_PHRASE_NO_MATCH	= 0,
	TP_PHRASE_MATCH		= 1,
	TP_PHRASE_BAD_INPUT = -1,
} TpPhraseResult;

/*
 * Tokenize 'doc_text' with the given text_config and verify whether
 * the (lowercased) phrase tokens appear in ordered adjacent
 * positions. Tokens are first run through to_tsvector_byid in a
 * single pass — that gives us each input token's stemmed form so
 * we can match it against the doc's stemmed tokens.
 *
 * If 'last_is_prefix' is true, the final token need not match a
 * stemmed indexed token verbatim — it matches any indexed token
 * that starts with the (post-stemming) prefix.
 *
 * Returns TP_PHRASE_MATCH if the phrase appears in the doc, else
 * TP_PHRASE_NO_MATCH.
 */
extern TpPhraseResult tp_phrase_verify_text(
		const char *doc_text,
		Oid			text_config_oid,
		char	  **phrase_tokens,
		int			phrase_token_count,
		bool		last_is_prefix,
		uint8		content_format);

/*
 * V6 fast path: positional verifier that takes pre-collected
 * per-token position lists directly, bypassing re-tokenization.
 *
 * positions_per_token[i] is an array of positions_count_per_token[i]
 * uint32 ordinals where stemmed phrase token i appears in the doc.
 * For last_is_prefix, positions_per_token[last] must be the union of
 * positions across all indexed tokens starting with the prefix (the
 * caller collects them via prefix expansion + per-term position
 * lookup).
 *
 * Arrays may be NULL (and counts 0) when a token has no occurrences
 * — the phrase trivially cannot match in that case.
 */
extern TpPhraseResult tp_phrase_verify_positions(
		uint32 **positions_per_token,
		int		*positions_count_per_token,
		int		 phrase_token_count);
