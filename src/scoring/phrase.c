/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * phrase.c - Phrase / phrase-prefix positional verifier
 *
 * See phrase.h for design overview.
 */
#include <postgres.h>

#include <fmgr.h>
#include <lib/stringinfo.h>
#include <tsearch/ts_type.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <varatt.h>

#include "scoring/phrase.h"
#include "types/markup.h"

/*
 * Stem a single query token through to_tsvector_byid so it matches
 * the indexed form. Returns a palloc'd cstring (may be empty if the
 * token reduces to a stopword).
 */
static char *
tp_stem_one(const char *token, Oid text_config_oid)
{
	Datum	   tsv_datum;
	TSVector   tsv;
	WordEntry *entries;
	char	  *lex_start;
	char	  *result;

	tsv_datum = DirectFunctionCall2(
			to_tsvector_byid,
			ObjectIdGetDatum(text_config_oid),
			PointerGetDatum(cstring_to_text(token)));
	tsv = DatumGetTSVector(tsv_datum);

	if (tsv->size == 0)
		return pstrdup("");

	entries	  = ARRPTR(tsv);
	lex_start = STRPTR(tsv);
	result	  = palloc(entries[0].len + 1);
	memcpy(result, lex_start + entries[0].pos, entries[0].len);
	result[entries[0].len] = '\0';
	return result;
}

TpPhraseResult
tp_phrase_verify_text(
		const char *doc_text,
		Oid			text_config_oid,
		char	  **phrase_tokens,
		int			phrase_token_count,
		bool		last_is_prefix,
		uint8		content_format)
{
	Datum	   tsv_datum;
	TSVector   tsv;
	WordEntry *entries;
	char	  *lex_start;
	char	 **stems	 = NULL;
	int		  *stem_lens = NULL;
	int		   i, j, k;
	uint32	 **positions_per_term		= NULL;
	int		  *positions_count_per_term = NULL;
	bool	   match					= false;

	if (doc_text == NULL || phrase_tokens == NULL || phrase_token_count == 0 ||
		!OidIsValid(text_config_oid))
		return TP_PHRASE_BAD_INPUT;

	/* Normalize and tokenize the document */
	{
		text *doc_normalized = tp_normalize_markup(
				cstring_to_text(doc_text), (TpContentFormat)content_format);
		tsv_datum = DirectFunctionCall2(
				to_tsvector_byid,
				ObjectIdGetDatum(text_config_oid),
				PointerGetDatum(doc_normalized));
	}
	tsv		  = DatumGetTSVector(tsv_datum);
	entries	  = ARRPTR(tsv);
	lex_start = STRPTR(tsv);

	if (tsv->size == 0)
		return TP_PHRASE_NO_MATCH;

	/*
	 * Stem each phrase token (last is left raw if it's a prefix).
	 * palloc0 so early-exit via `goto cleanup` doesn't read garbage
	 * from uninitialized slots when freeing.
	 */
	stems	  = palloc0(phrase_token_count * sizeof(char *));
	stem_lens = palloc0(phrase_token_count * sizeof(int));
	for (i = 0; i < phrase_token_count; i++)
	{
		bool is_last_prefix = (last_is_prefix && i == phrase_token_count - 1);
		if (is_last_prefix)
			stems[i] = pstrdup(phrase_tokens[i]); /* raw prefix */
		else
			stems[i] = tp_stem_one(phrase_tokens[i], text_config_oid);
		stem_lens[i] = (int)strlen(stems[i]);

		/* Empty stem (stopword) for non-prefix terms = phrase can't match */
		if (stem_lens[i] == 0 && !is_last_prefix)
		{
			match = false;
			goto cleanup;
		}
	}

	/*
	 * Build positions_per_term[i] = array of position ordinals in
	 * the doc where stem i (or, for last_is_prefix, any token
	 * starting with the prefix) appears. If any term has zero
	 * positions, the phrase can't match — short-circuit.
	 */
	positions_per_term		 = palloc0(phrase_token_count * sizeof(uint32 *));
	positions_count_per_term = palloc0(phrase_token_count * sizeof(int));

	for (i = 0; i < phrase_token_count; i++)
	{
		bool is_last_prefix = (last_is_prefix && i == phrase_token_count - 1);
		int	 doc_term_count = tsv->size;
		int	 capacity		= 8;
		uint32 *posbuf		= palloc(capacity * sizeof(uint32));
		int		nposes		= 0;

		for (j = 0; j < doc_term_count; j++)
		{
			char *doc_lex	  = lex_start + entries[j].pos;
			int	  doc_lex_len = entries[j].len;
			bool  match_this  = false;

			if (is_last_prefix)
			{
				if (doc_lex_len >= stem_lens[i] &&
					memcmp(doc_lex, stems[i], stem_lens[i]) == 0)
					match_this = true;
			}
			else if (
					doc_lex_len == stem_lens[i] &&
					memcmp(doc_lex, stems[i], stem_lens[i]) == 0)
			{
				match_this = true;
			}

			if (!match_this)
				continue;

			if (entries[j].haspos)
			{
				int			  npos	  = (int)POSDATALEN(tsv, &entries[j]);
				WordEntryPos *posdata = POSDATAPTR(tsv, &entries[j]);
				int			  pp;

				if (nposes + npos > capacity)
				{
					while (capacity < nposes + npos)
						capacity *= 2;
					posbuf = repalloc(posbuf, capacity * sizeof(uint32));
				}
				for (pp = 0; pp < npos; pp++)
					posbuf[nposes++] = (uint32)WEP_GETPOS(posdata[pp]);
			}
			else
			{
				/*
				 * No position data for this entry — extremely rare
				 * for to_tsvector output (it always produces
				 * positions). Treat as position 1 (best effort).
				 */
				if (nposes >= capacity)
				{
					capacity *= 2;
					posbuf = repalloc(posbuf, capacity * sizeof(uint32));
				}
				posbuf[nposes++] = 1;
			}
		}

		if (nposes == 0)
		{
			pfree(posbuf);
			match = false;
			goto cleanup;
		}

		/* Sort positions ascending — to_tsvector emits sorted within
		 * a single WordEntry but across multiple matched entries
		 * (only happens for last_is_prefix) we need a global sort. */
		if (is_last_prefix && nposes > 1)
		{
			/* Simple insertion sort — usually small */
			int a, b;
			for (a = 1; a < nposes; a++)
			{
				uint32 v = posbuf[a];
				b		 = a - 1;
				while (b >= 0 && posbuf[b] > v)
				{
					posbuf[b + 1] = posbuf[b];
					b--;
				}
				posbuf[b + 1] = v;
			}
		}

		positions_per_term[i]		= posbuf;
		positions_count_per_term[i] = nposes;
	}

	/*
	 * Verify ordered adjacency. For each candidate start position s
	 * in positions_per_term[0], check whether s+1 ∈ positions[1],
	 * s+2 ∈ positions[2], ..., s+(n-1) ∈ positions[n-1]. We use a
	 * linear scan since position lists are small (typically ≤ 5).
	 */
	for (i = 0; i < positions_count_per_term[0] && !match; i++)
	{
		uint32 start = positions_per_term[0][i];
		bool   ok	 = true;
		for (k = 1; k < phrase_token_count; k++)
		{
			uint32 want	 = start + (uint32)k;
			bool   found = false;
			int	   m;
			for (m = 0; m < positions_count_per_term[k]; m++)
			{
				if (positions_per_term[k][m] == want)
				{
					found = true;
					break;
				}
				if (positions_per_term[k][m] > want)
					break; /* sorted, can't find later */
			}
			if (!found)
			{
				ok = false;
				break;
			}
		}
		if (ok)
			match = true;
	}

cleanup:
	if (positions_per_term)
	{
		for (i = 0; i < phrase_token_count; i++)
		{
			if (positions_per_term[i])
				pfree(positions_per_term[i]);
		}
		pfree(positions_per_term);
	}
	if (positions_count_per_term)
		pfree(positions_count_per_term);
	if (stems)
	{
		for (i = 0; i < phrase_token_count; i++)
			if (stems[i])
				pfree(stems[i]);
		pfree(stems);
	}
	if (stem_lens)
		pfree(stem_lens);

	return match ? TP_PHRASE_MATCH : TP_PHRASE_NO_MATCH;
}

TpPhraseResult
tp_phrase_verify_positions(
		uint32 **positions_per_token,
		int		*positions_count_per_token,
		int		 phrase_token_count)
{
	int i, k;

	if (positions_per_token == NULL || positions_count_per_token == NULL ||
		phrase_token_count <= 0)
		return TP_PHRASE_BAD_INPUT;

	/* Any token with zero positions short-circuits to no match. */
	for (i = 0; i < phrase_token_count; i++)
	{
		if (positions_per_token[i] == NULL ||
			positions_count_per_token[i] == 0)
			return TP_PHRASE_NO_MATCH;
	}

	/*
	 * Same ordered-adjacency scan as tp_phrase_verify_text. Position
	 * lists are expected to be ascending; if a caller supplies
	 * unsorted lists (e.g., union across prefix expansions), they
	 * must sort before calling.
	 */
	for (i = 0; i < positions_count_per_token[0]; i++)
	{
		uint32 start = positions_per_token[0][i];
		bool   ok	 = true;
		for (k = 1; k < phrase_token_count; k++)
		{
			uint32 want	 = start + (uint32)k;
			bool   found = false;
			int	   m;
			for (m = 0; m < positions_count_per_token[k]; m++)
			{
				if (positions_per_token[k][m] == want)
				{
					found = true;
					break;
				}
				if (positions_per_token[k][m] > want)
					break;
			}
			if (!found)
			{
				ok = false;
				break;
			}
		}
		if (ok)
			return TP_PHRASE_MATCH;
	}
	return TP_PHRASE_NO_MATCH;
}
