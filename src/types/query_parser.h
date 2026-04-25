/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * query_parser.h - Query grammar parser for bm25query
 *
 * Query grammar:
 *   foo              → regular term (tokenized/stemmed by text_config)
 *   foo*             → prefix term (expanded via index dictionary)
 *   "foo bar"        → ordered-adjacent phrase
 *   "foo bar*"       → phrase-prefix
 *   field:foo        → field-scoped clause
 *   field:(foo bar*) → grouped field scope
 *   foo bar          → implicit OR of terms/phrases
 *
 * Backslash escapes: \: \* \" \( \) \\
 */
#pragma once

#include <postgres.h>

#include <storage/block.h>
#include <utils/rel.h>

#include "index/state.h"
#include "types/fuzzy.h"

typedef enum
{
	TP_QTERM_TERM,	 /* regular term — tokenize via text_config */
	TP_QTERM_PREFIX, /* prefix term — strip *, lowercase, dict-expand */
	TP_QTERM_PHRASE, /* "foo bar" — quoted ordered-adjacent sequence */
	TP_QTERM_PHRASE_PREFIX, /* "foo bar*" — phrase whose final token is a
							   prefix */
} TpQueryTermKind;

/*
 * One clause from the parsed query string.
 *
 * For TP_QTERM_TERM: 'text' is the raw token. Tokenized via the
 *   index's text_config at resolution time.
 *
 * For TP_QTERM_PREFIX: 'text' is the lowercased prefix with the
 *   trailing '*' stripped. Matched against indexed dictionary terms
 *   verbatim (no further stemming).
 *
 * For TP_QTERM_PHRASE / TP_QTERM_PHRASE_PREFIX: 'text' is the raw
 *   contents between the quotes. 'phrase_tokens' is the
 *   whitespace-split token list (each token lowercased; for
 *   PHRASE_PREFIX the final token's trailing '*' is stripped). Phrase
 *   verification at scan time tokenizes the matching document with
 *   the same text_config and checks positional ordered adjacency.
 */
typedef struct TpQueryTerm
{
	TpQueryTermKind kind;
	char		   *text; /* palloc'd, null-terminated */
	int				len;

	/* Phrase-only fields (NULL/0 for term/prefix) */
	char **phrase_tokens; /* palloc'd array; each token palloc'd cstring */
	int	   phrase_token_count;

	/*
	 * Multi-column (Phase 6.1). field_name is non-NULL when the
	 * clause was written as `field:token` / `field:"phrase"` /
	 * `field:prefix*`, or inherited from a flattened `field:(...)`
	 * group. NULL means unqualified (expands to OR across all indexed
	 * fields at resolution time).
	 */
	char *field_name;
} TpQueryTerm;

typedef struct TpParsedQuery
{
	int			 term_count;
	TpQueryTerm *terms; /* palloc'd array */
} TpParsedQuery;

/*
 * Cap on prefix expansions per prefix term. Registered as a
 * GUC (pg_textsearch.max_prefix_expansions, default 50).
 */
extern int tp_max_prefix_expansions;

/*
 * Multiplier on the requested top-K result count when generating
 * BMW candidates for phrase / phrase-prefix queries. Each candidate
 * is verified by re-tokenizing the heap row, so most candidates
 * survive only when the phrase is rare. Higher overfetch trades
 * extra heap fetches for better recall on highly-skewed phrases.
 *
 * Registered as pg_textsearch.phrase_candidate_overfetch (default 4).
 */
extern int tp_phrase_candidate_overfetch;

/*
 * Parse a query string into TpParsedQuery.
 * Errors on syntax problems (e.g. lone '*', empty prefix).
 * Caller must free with tp_free_parsed_query().
 */
extern TpParsedQuery *tp_parse_query(const char *query_text);
extern void			  tp_free_parsed_query(TpParsedQuery *pq);

/*
 * Cheap lookahead: returns true iff the query string contains any
 * grammar extension token (field scope, prefix, quoted phrase,
 * grouping, or escape).
 * Allows the scan path to fast-path simple queries through the
 * existing tsvector path without parsing.
 */
extern bool tp_query_has_grammar_extension(const char *query_text);

/* Returns true iff parsed query has any phrase or phrase-prefix clause. */
extern bool tp_parsed_query_has_phrase(const TpParsedQuery *pq);

/*
 * Expand a prefix term against the live index (memtable + all segment
 * levels). Returns the number of unique concrete terms found, capped
 * at max_expansions. Caller owns the returned array; both the array
 * and each individual string are palloc'd in the current context.
 *
 * If the prefix has zero matches, sets *out_terms = NULL and returns 0.
 */
extern int tp_expand_prefix(
		TpLocalIndexState *state,
		Relation		   index,
		BlockNumber		  *level_heads,
		const char		  *prefix,
		int				   max_expansions,
		char			***out_terms);

extern int tp_expand_fuzzy(
		TpLocalIndexState *state,
		Relation		   index,
		BlockNumber		  *level_heads,
		const char		  *query_term,
		int				   max_distance,
		int				   max_expansions,
		bool			   prefix,
		TpFuzzyCandidate **out_candidates);

/*
 * Resolve a parsed query into a flat list of concrete terms +
 * frequencies. Regular terms are routed through to_tsvector_byid
 * with the index's text_config (handles word breaks, stopwords,
 * stemming). Prefix terms are dictionary-expanded against the live
 * index. Output arrays are palloc'd in the current context; caller
 * pfrees individual terms, the terms array, and the freqs array.
 *
 * Returns the count of resolved terms (may be 0 if everything was
 * a stopword or had zero prefix matches).
 */
/*
 * metap: optional metapage for multi-column field registry. When
 * metap->num_fields > 1, field-scoped clauses (`title:term`) resolve
 * to their field's tag-prefixed stem, and unqualified clauses expand
 * to an OR across every indexed field. Pass NULL for single-column
 * indexes (legacy untagged behavior).
 */
struct TpIndexMetaPageData;
extern int tp_resolve_query_terms(
		TpParsedQuery					 *pq,
		TpLocalIndexState				 *state,
		Relation						  index,
		BlockNumber						 *level_heads,
		Oid								  text_config_oid,
		const struct TpIndexMetaPageData *metap,
		char						   ***out_terms,
		int32							**out_freqs);

extern int tp_resolve_query_terms_ex(
		TpParsedQuery					 *pq,
		TpLocalIndexState				 *state,
		Relation						  index,
		BlockNumber						 *level_heads,
		Oid								  text_config_oid,
		const struct TpIndexMetaPageData *metap,
		bool							  fuzzy,
		int								  fuzzy_max_distance,
		char						   ***out_terms,
		int32							**out_freqs,
		float4							**out_weights);
