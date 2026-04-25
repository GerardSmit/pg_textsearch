/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * fuzzy.c - Fuzzy query helpers
 */
#include <postgres.h>

#include <limits.h>
#include <mb/pg_wchar.h>
#include <stdlib.h>
#include <utils/builtins.h>

#include "constants.h"
#include "types/fuzzy.h"

int tp_max_fuzzy_expansions = 50;
int tp_max_fuzzy_distance	= 2;
int tp_min_fuzzy_term_len	= 3;

typedef struct TpFuzzyChar
{
	const char *ptr;
	int			len;
} TpFuzzyChar;

static int
tp_term_lexical_offset(const char *term, uint8 *field_tag)
{
	unsigned char first = (unsigned char)term[0];

	if (first >= TP_FIELD_TAG_BASE)
	{
		if (field_tag != NULL)
			*field_tag = first;
		return 1;
	}

	if (field_tag != NULL)
		*field_tag = 0;
	return 0;
}

static int
tp_collect_chars(const char *s, int len, TpFuzzyChar **out_chars)
{
	TpFuzzyChar *chars;
	const char	*p		  = s;
	const char	*end	  = s + len;
	int			 count	  = 0;
	int			 capacity = 8;

	if (len <= 0)
	{
		*out_chars = NULL;
		return 0;
	}

	chars = palloc(capacity * sizeof(TpFuzzyChar));
	while (p < end)
	{
		int clen = pg_mblen(p);

		if (clen <= 0 || p + clen > end)
			clen = 1;

		if (count >= capacity)
		{
			capacity *= 2;
			chars = repalloc(chars, capacity * sizeof(TpFuzzyChar));
		}

		chars[count].ptr = p;
		chars[count].len = clen;
		count++;
		p += clen;
	}

	*out_chars = chars;
	return count;
}

static bool
tp_chars_equal(const TpFuzzyChar *a, const TpFuzzyChar *b)
{
	return a->len == b->len && memcmp(a->ptr, b->ptr, a->len) == 0;
}

static int
tp_chars_byte_len(const TpFuzzyChar *chars, int count)
{
	if (count <= 0)
		return 0;
	return (int)((chars[count - 1].ptr + chars[count - 1].len) - chars[0].ptr);
}

int
tp_edit_distance_bounded(
		const char *a, int a_len, const char *b, int b_len, int max_distance)
{
	TpFuzzyChar *ac = NULL;
	TpFuzzyChar *bc = NULL;
	int			 n;
	int			 m;
	int			*prevprev = NULL;
	int			*prev	  = NULL;
	int			*curr	  = NULL;
	int			 i, j;
	int			 result;

	if (max_distance < 0)
		return 0;

	n = tp_collect_chars(a, a_len, &ac);
	m = tp_collect_chars(b, b_len, &bc);

	if (abs(n - m) > max_distance)
	{
		if (ac)
			pfree(ac);
		if (bc)
			pfree(bc);
		return max_distance + 1;
	}

	prevprev = palloc0((m + 1) * sizeof(int));
	prev	 = palloc((m + 1) * sizeof(int));
	curr	 = palloc((m + 1) * sizeof(int));

	for (j = 0; j <= m; j++)
		prev[j] = j;

	for (i = 1; i <= n; i++)
	{
		int row_min = INT_MAX;

		curr[0] = i;
		if (curr[0] < row_min)
			row_min = curr[0];

		for (j = 1; j <= m; j++)
		{
			int cost = tp_chars_equal(&ac[i - 1], &bc[j - 1]) ? 0 : 1;
			int v = Min(Min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);

			if (i > 1 && j > 1 && tp_chars_equal(&ac[i - 1], &bc[j - 2]) &&
				tp_chars_equal(&ac[i - 2], &bc[j - 1]))
				v = Min(v, prevprev[j - 2] + 1);

			curr[j] = v;
			if (v < row_min)
				row_min = v;
		}

		if (row_min > max_distance)
		{
			result = max_distance + 1;
			goto done;
		}

		{
			int *tmp = prevprev;
			prevprev = prev;
			prev	 = curr;
			curr	 = tmp;
		}
	}

	result = prev[m];
	if (result > max_distance)
		result = max_distance + 1;

done:
	if (ac)
		pfree(ac);
	if (bc)
		pfree(bc);
	pfree(prevprev);
	pfree(prev);
	pfree(curr);
	return result;
}

int
tp_fuzzy_lexical_char_len(const char *term)
{
	uint8		 tag;
	int			 off   = tp_term_lexical_offset(term, &tag);
	const char	*lex   = term + off;
	TpFuzzyChar *chars = NULL;
	int			 count = tp_collect_chars(lex, strlen(lex), &chars);

	if (chars)
		pfree(chars);
	return count;
}

void
tp_fuzzy_fill_term_meta(const char *term, TpFuzzyTermMeta *meta)
{
	uint8 tag = 0;

	tp_term_lexical_offset(term, &tag);
	memset(meta, 0, sizeof(TpFuzzyTermMeta));
	meta->lexical_chars = (uint32)tp_fuzzy_lexical_char_len(term);
	meta->field_tag		= tag;
}

bool
tp_fuzzy_match_term(
		const char *query_term,
		const char *candidate_term,
		int			max_distance,
		bool		prefix,
		uint8	   *out_distance)
{
	uint8		 query_tag	   = 0;
	uint8		 candidate_tag = 0;
	int			 query_off;
	int			 candidate_off;
	const char	*query_lex;
	const char	*candidate_lex;
	int			 query_len;
	int			 candidate_len;
	TpFuzzyChar *query_chars	 = NULL;
	TpFuzzyChar *candidate_chars = NULL;
	int			 query_char_count;
	int			 candidate_char_count;
	int			 best = max_distance + 1;
	int			 min_chars;
	int			 max_chars;
	int			 c;

	query_off	  = tp_term_lexical_offset(query_term, &query_tag);
	candidate_off = tp_term_lexical_offset(candidate_term, &candidate_tag);

	if (query_tag != candidate_tag)
		return false;

	query_lex	  = query_term + query_off;
	candidate_lex = candidate_term + candidate_off;
	query_len	  = strlen(query_lex);
	candidate_len = strlen(candidate_lex);

	query_char_count = tp_collect_chars(query_lex, query_len, &query_chars);
	candidate_char_count =
			tp_collect_chars(candidate_lex, candidate_len, &candidate_chars);

	if (query_char_count == 0 || candidate_char_count == 0)
		goto done;

	if (!prefix)
	{
		if (abs(query_char_count - candidate_char_count) <= max_distance)
			best = tp_edit_distance_bounded(
					query_lex,
					query_len,
					candidate_lex,
					candidate_len,
					max_distance);
		goto done;
	}

	if (candidate_char_count + max_distance < query_char_count)
		goto done;

	min_chars = Max(1, query_char_count - max_distance);
	max_chars = Min(candidate_char_count, query_char_count + max_distance);
	for (c = min_chars; c <= max_chars; c++)
	{
		int prefix_len = tp_chars_byte_len(candidate_chars, c);
		int dist	   = tp_edit_distance_bounded(
				  query_lex, query_len, candidate_lex, prefix_len, max_distance);

		if (dist < best)
			best = dist;
		if (best == 0)
			break;
	}

done:
	if (query_chars)
		pfree(query_chars);
	if (candidate_chars)
		pfree(candidate_chars);

	if (best <= max_distance)
	{
		if (out_distance != NULL)
			*out_distance = (uint8)best;
		return true;
	}
	return false;
}

static int
tp_fuzzy_candidate_cmp(const TpFuzzyCandidate *a, const TpFuzzyCandidate *b)
{
	if (a->distance != b->distance)
		return (int)a->distance - (int)b->distance;
	return strcmp(a->term, b->term);
}

void
tp_fuzzy_candidates_insert(
		TpFuzzyCandidate **candidates,
		int				  *count,
		int				  *capacity,
		char			  *term,
		uint8			   distance,
		int				   max_terms)
{
	int i, pos;

	if (max_terms <= 0)
	{
		pfree(term);
		return;
	}

	for (i = 0; i < *count; i++)
	{
		int cmp = strcmp((*candidates)[i].term, term);
		if (cmp == 0)
		{
			if (distance < (*candidates)[i].distance)
				(*candidates)[i].distance = distance;
			pfree(term);
			goto sort_and_trim;
		}
	}

	if (*count < max_terms)
	{
		if (*count >= *capacity)
		{
			*capacity = (*capacity == 0) ? 8 : *capacity * 2;
			if (*capacity > max_terms)
				*capacity = max_terms;
			if (*candidates == NULL)
				*candidates = palloc(*capacity * sizeof(TpFuzzyCandidate));
			else
				*candidates = repalloc(
						*candidates, *capacity * sizeof(TpFuzzyCandidate));
		}
		(*candidates)[*count].term	   = term;
		(*candidates)[*count].distance = distance;
		(*count)++;
	}
	else
	{
		TpFuzzyCandidate incoming;
		incoming.term	  = term;
		incoming.distance = distance;
		if (tp_fuzzy_candidate_cmp(&incoming, &(*candidates)[*count - 1]) >= 0)
		{
			pfree(term);
			return;
		}
		pfree((*candidates)[*count - 1].term);
		(*candidates)[*count - 1] = incoming;
	}

sort_and_trim:
	for (i = 1; i < *count; i++)
	{
		TpFuzzyCandidate key = (*candidates)[i];
		pos					 = i - 1;
		while (pos >= 0 &&
			   tp_fuzzy_candidate_cmp(&(*candidates)[pos], &key) > 0)
		{
			(*candidates)[pos + 1] = (*candidates)[pos];
			pos--;
		}
		(*candidates)[pos + 1] = key;
	}
}

void
tp_fuzzy_candidates_free(TpFuzzyCandidate *candidates, int count)
{
	int i;

	if (candidates == NULL)
		return;
	for (i = 0; i < count; i++)
		pfree(candidates[i].term);
	pfree(candidates);
}
