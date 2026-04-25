/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * fuzzy.h - Fuzzy query helpers
 */
#pragma once

#include <postgres.h>

#include "segment/format.h"

typedef struct TpFuzzyCandidate
{
	char *term;
	uint8 distance;
} TpFuzzyCandidate;

extern int tp_max_fuzzy_expansions;
extern int tp_max_fuzzy_distance;
extern int tp_min_fuzzy_term_len;

extern int tp_edit_distance_bounded(
		const char *a, int a_len, const char *b, int b_len, int max_distance);

extern int	tp_fuzzy_lexical_char_len(const char *term);
extern void tp_fuzzy_fill_term_meta(const char *term, TpFuzzyTermMeta *meta);

extern bool tp_fuzzy_match_term(
		const char *query_term,
		const char *candidate_term,
		int			max_distance,
		bool		prefix,
		uint8	   *out_distance);

extern void tp_fuzzy_candidates_insert(
		TpFuzzyCandidate **candidates,
		int				  *count,
		int				  *capacity,
		char			  *term,
		uint8			   distance,
		int				   max_terms);

extern void tp_fuzzy_candidates_free(TpFuzzyCandidate *candidates, int count);
