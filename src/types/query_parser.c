/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * query_parser.c - Query grammar parser for bm25query (Phase 1: prefix)
 */
#include <postgres.h>

#include <ctype.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <storage/itemptr.h>
#include <tsearch/ts_type.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <varatt.h>

#include "constants.h"
#include "index/metapage.h"
#include "index/state.h"
#include "memtable/stringtable.h"
#include "segment/segment.h"
#include "types/fuzzy.h"
#include "types/query_parser.h"

int tp_max_prefix_expansions	  = 50;
int tp_phrase_candidate_overfetch = 4;

static float4
tp_fuzzy_distance_weight(uint8 distance)
{
	if (distance == 0)
		return 1.0f;
	if (distance == 1)
		return 0.8f;
	if (distance == 2)
		return 0.6f;
	return Max(0.2f, 1.0f - (0.2f * (float4)distance));
}

/* ------------------------------------------------------------------ */
/* Tokenizer                                                          */
/* ------------------------------------------------------------------ */

static inline bool
tp_is_query_space(char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' ||
		   c == '\v';
}

static inline bool
tp_is_escapable_query_char(char c)
{
	return c == ':' || c == '*' || c == '"' || c == '(' || c == ')' ||
		   c == '\\';
}

static void
tp_validate_escape(const char *p)
{
	if (p[1] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("trailing backslash in query"),
				 errhint("Write '\\\\' to search for a literal backslash.")));

	if (!tp_is_escapable_query_char(p[1]))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid escape sequence in query"),
				 errhint("Only ':', '*', '\"', '(', ')', and '\\' can be "
						 "escaped.")));
}

static char *
tp_decode_escaped_range(const char *start, int len, int *out_len)
{
	char *decoded;
	int	  i;
	int	  j = 0;

	decoded = palloc(len + 1);
	for (i = 0; i < len; i++)
	{
		if (start[i] == '\\')
		{
			tp_validate_escape(start + i);
			if (i + 1 >= len)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("trailing backslash in query"),
						 errhint("Write '\\\\' to search for a literal "
								 "backslash.")));
			decoded[j++] = start[++i];
		}
		else
		{
			decoded[j++] = start[i];
		}
	}
	decoded[j] = '\0';
	if (out_len)
		*out_len = j;
	return decoded;
}

/*
 * Lowercase a buffer in place using ASCII rules. Multi-byte UTF-8
 * sequences pass through untouched (already lowercased indexed terms
 * for non-ASCII scripts come from the same tokenizer; for ASCII we
 * mirror to_tsvector's case-fold behavior). This is a pragmatic
 * approximation; full Unicode case-folding lives in upstream collation
 * services and would require a per-call OS-locale call. Sufficient
 * for English/Latin prefixes which are the dominant prefix-search use
 * case; non-Latin prefixes match dict terms verbatim.
 */
static void
tp_lowercase_inplace(char *s, int len)
{
	int i;

	for (i = 0; i < len; i++)
	{
		unsigned char c = (unsigned char)s[i];
		if (c < 128 && isupper(c))
			s[i] = (char)tolower(c);
	}
}

bool
tp_query_has_grammar_extension(const char *query_text)
{
	const char *p;

	if (query_text == NULL)
		return false;
	for (p = query_text; *p;)
	{
		if (*p == '\\')
			return true;
		if (*p == '*' || *p == '"' || *p == ':' || *p == '(' || *p == ')')
			return true;
		p++;
	}
	return false;
}

/*
 * Try to parse a `field_name:` prefix at *pp. Field name is a
 * C-identifier-shaped run ([A-Za-z_][A-Za-z0-9_]*) immediately
 * followed by `:`. On match, returns a palloc'd copy of the name
 * and advances *pp past the colon. On no match, returns NULL and
 * leaves *pp unchanged.
 */
static char *
tp_try_parse_field_prefix(const char **pp)
{
	const char *p	  = *pp;
	const char *start = p;
	int			name_len;
	char	   *name;

	if (!*p)
		return NULL;
	{
		unsigned char c = (unsigned char)*p;
		if (!(isalpha(c) || c == '_'))
			return NULL;
	}
	p++;
	while (*p)
	{
		unsigned char c = (unsigned char)*p;
		if (isalnum(c) || c == '_')
			p++;
		else
			break;
	}
	if (*p != ':')
		return NULL;

	name_len = (int)(p - start);
	if (name_len == 0)
		return NULL;

	name = palloc(name_len + 1);
	memcpy(name, start, name_len);
	name[name_len] = '\0';
	*pp			   = p + 1; /* consume colon */
	return name;
}

bool
tp_parsed_query_has_phrase(const TpParsedQuery *pq)
{
	int i;

	if (pq == NULL)
		return false;
	for (i = 0; i < pq->term_count; i++)
	{
		if (pq->terms[i].kind == TP_QTERM_PHRASE ||
			pq->terms[i].kind == TP_QTERM_PHRASE_PREFIX)
			return true;
	}
	return false;
}

/*
 * Helper: parse a quoted phrase body into its component tokens.
 * 'body' is the raw text between the surrounding quotes (no quotes).
 * Whitespace separates tokens. Final '*' on the last token marks
 * phrase-prefix. Each emitted token is lowercased.
 *
 * Errors on empty phrase or unescaped embedded '*' in non-final token.
 * Backslash escapes are decoded after deciding which grammar characters
 * are real syntax, so escaped '*' remains literal text instead of a
 * phrase-prefix marker.
 *
 * Returns a freshly palloc'd array; caller pfrees individual tokens
 * + the array.
 */
static void
tp_split_phrase_tokens(
		const char *body,
		int			body_len,
		char	 ***out_tokens,
		int		   *out_count,
		bool	   *out_last_is_prefix)
{
	char **tokens		 = NULL;
	bool  *trailing_star = NULL;
	int	  *star_count	 = NULL;
	int	   capacity		 = 4;
	int	   count		 = 0;
	int	   i			 = 0;
	bool   last_pref	 = false;

	tokens		  = palloc(capacity * sizeof(char *));
	trailing_star = palloc(capacity * sizeof(bool));
	star_count	  = palloc(capacity * sizeof(int));

	while (i < body_len)
	{
		int	  start;
		int	  len;
		int	  decoded_len;
		int	  stars			= 0;
		bool  last_was_star = false;
		char *t;

		while (i < body_len && tp_is_query_space(body[i]))
			i++;
		if (i >= body_len)
			break;

		start = i;
		while (i < body_len && !tp_is_query_space(body[i]))
		{
			if (body[i] == '\\')
			{
				tp_validate_escape(body + i);
				i += 2;
				last_was_star = false;
				continue;
			}
			if (body[i] == '*')
			{
				stars++;
				last_was_star = true;
			}
			else
			{
				last_was_star = false;
			}
			i++;
		}
		len = i - start;

		if (count >= capacity)
		{
			capacity *= 2;
			tokens		  = repalloc(tokens, capacity * sizeof(char *));
			trailing_star = repalloc(trailing_star, capacity * sizeof(bool));
			star_count	  = repalloc(star_count, capacity * sizeof(int));
		}

		t = tp_decode_escaped_range(body + start, len, &decoded_len);
		tp_lowercase_inplace(t, decoded_len);
		tokens[count]		 = t;
		trailing_star[count] = last_was_star;
		star_count[count]	 = stars;
		count++;
	}

	if (count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("empty phrase in query"),
				 errhint("Quoted phrases must contain at least one token.")));

	/* Detect unescaped trailing '*' on the LAST token only */
	{
		char *last	   = tokens[count - 1];
		int	  last_len = (int)strlen(last);

		if (trailing_star[count - 1])
		{
			last[--last_len] = '\0';
			if (last_len == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("empty prefix in phrase-prefix"),
						 errhint("Phrase-prefix queries require at least one "
								 "character before '*'.")));
			last_pref = true;
		}
	}

	/* Validate no embedded unescaped '*' in any token */
	{
		int t;
		for (t = 0; t < count; t++)
		{
			int allowed = (t == count - 1 && trailing_star[t]) ? 1 : 0;
			if (star_count[t] > allowed)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("'*' in phrase is only allowed as the "
								"trailing prefix marker on the final "
								"token")));
		}
	}

	*out_tokens			= tokens;
	*out_count			= count;
	*out_last_is_prefix = last_pref;
	pfree(trailing_star);
	pfree(star_count);
}

static void
tp_ensure_term_capacity(TpQueryTerm **terms, int *capacity, int count)
{
	if (count >= *capacity)
	{
		*capacity *= 2;
		*terms = repalloc(*terms, *capacity * sizeof(TpQueryTerm));
	}
}

static const char *
tp_find_phrase_closing_quote(const char *p)
{
	while (*p)
	{
		if (*p == '\\')
		{
			tp_validate_escape(p);
			p += 2;
			continue;
		}
		if (*p == '"')
			return p;
		p++;
	}
	return NULL;
}

static const char *
tp_scan_bare_token(
		const char *p,
		bool		in_group,
		bool	   *out_trailing_star,
		int		   *out_star_count)
{
	bool trailing_star = false;
	int	 stars		   = 0;

	while (*p)
	{
		if (*p == '\\')
		{
			tp_validate_escape(p);
			p += 2;
			trailing_star = false;
			continue;
		}
		if (tp_is_query_space(*p) || *p == '"' || *p == '(' ||
			(in_group && *p == ')'))
			break;
		if (*p == ')')
			break;
		if (*p == '*')
		{
			stars++;
			trailing_star = true;
		}
		else
		{
			trailing_star = false;
		}
		p++;
	}

	*out_trailing_star = trailing_star;
	*out_star_count	   = stars;
	return p;
}

static void
tp_parse_query_clauses(
		const char	**pp,
		TpQueryTerm **terms,
		int			 *capacity,
		int			 *count,
		const char	 *inherited_field,
		bool		  in_group)
{
	const char *p = *pp;

	while (*p)
	{
		char *field_name = NULL;

		/* Skip leading whitespace */
		while (*p && tp_is_query_space(*p))
			p++;
		if (!*p)
			break;

		if (in_group && *p == ')')
		{
			*pp = p + 1;
			return;
		}
		if (*p == ')')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unbalanced ')' in query"),
					 errhint("Escape ')' as '\\)' to search for a literal "
							 "parenthesis.")));
		if (*p == '(')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unexpected '(' in query"),
					 errhint("Use 'field:(...)' for grouped field scope, or "
							 "escape '(' as '\\(' to search for a literal "
							 "parenthesis.")));

		/* Optional `field_name:` prefix for multi-column scoping. */
		{
			const char *field_p = p;
			field_name			= tp_try_parse_field_prefix(&field_p);
			if (field_name != NULL && inherited_field != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("nested field scopes are not supported"),
						 errhint("Close the current group before starting "
								 "another field scope.")));
			if (field_name != NULL)
				p = field_p;
		}

		/* `field:` followed by whitespace/end is invalid. */
		if (field_name != NULL && (!*p || tp_is_query_space(*p) || *p == ')'))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("field-scoped query '%s:' has no token",
							field_name),
					 errhint("Write '%s:term', '%s:prefix*', or "
							 "'%s:\"phrase\"'.",
							 field_name,
							 field_name,
							 field_name)));

		if (field_name != NULL && *p == '(')
		{
			int group_start_count = *count;

			p++;
			if (*p == ')')
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("empty field group in query"),
						 errhint("Field groups must contain at least one "
								 "clause.")));
			tp_parse_query_clauses(
					&p, terms, capacity, count, field_name, true);
			if (*count == group_start_count)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("empty field group in query"),
						 errhint("Field groups must contain at least one "
								 "clause.")));
			pfree(field_name);
			continue;
		}

		if (*p == '"')
		{
			/* Phrase clause: scan until closing quote */
			const char	*body_start;
			const char	*closing;
			int			 body_len;
			char	   **phrase_tokens;
			int			 phrase_count;
			bool		 last_is_prefix;
			char		*raw;
			int			 raw_len;
			TpQueryTerm *term;

			p++; /* consume opening quote */
			body_start = p;
			closing	   = tp_find_phrase_closing_quote(p);
			if (closing == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("unterminated quoted phrase in query"),
						 errhint("Add a closing '\"' to terminate the "
								 "phrase.")));
			body_len = (int)(closing - body_start);
			p		 = closing + 1; /* consume closing quote */

			tp_split_phrase_tokens(
					body_start,
					body_len,
					&phrase_tokens,
					&phrase_count,
					&last_is_prefix);

			raw = tp_decode_escaped_range(body_start, body_len, &raw_len);

			tp_ensure_term_capacity(terms, capacity, *count);
			term = &(*terms)[*count];
			memset(term, 0, sizeof(TpQueryTerm));
			term->kind				 = last_is_prefix ? TP_QTERM_PHRASE_PREFIX
													  : TP_QTERM_PHRASE;
			term->text				 = raw;
			term->len				 = raw_len;
			term->phrase_tokens		 = phrase_tokens;
			term->phrase_token_count = phrase_count;
			term->field_name		 = field_name ? field_name
												  : (inherited_field
															 ? pstrdup(inherited_field)
															 : NULL);
			(*count)++;
			continue;
		}

		/* Bareword token: term or prefix */
		{
			const char	*start;
			const char	*end;
			int			 tok_len;
			int			 text_len;
			bool		 is_prefix = false;
			bool		 trailing_star;
			int			 star_count;
			char		*text;
			TpQueryTerm *term;

			start = p;
			end = tp_scan_bare_token(p, in_group, &trailing_star, &star_count);
			tok_len = (int)(end - start);

			if (tok_len == 0)
			{
				if (*p == '(')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unexpected '(' in query"),
							 errhint("Use 'field:(...)' for grouped field "
									 "scope, or escape '(' as '\\(' to search "
									 "for a literal parenthesis.")));
				if (*p == ')')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unbalanced ')' in query"),
							 errhint("Escape ')' as '\\)' to search for a "
									 "literal parenthesis.")));
			}

			/* Detect unescaped trailing '*' as prefix marker */
			if (trailing_star)
			{
				is_prefix = true;
				tok_len--; /* exclude trailing '*' from text */

				if (tok_len == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("empty prefix in query"),
							 errhint("Prefix queries require at least one "
									 "character before '*'.")));
			}

			/* Reject embedded unescaped '*' (only trailing supported) */
			{
				int allowed = is_prefix ? 1 : 0;
				if (star_count > allowed)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("'*' is only allowed as a trailing "
									"prefix marker"),
							 errhint("Use 'prefix*' to match terms "
									 "starting with the prefix.")));
			}

			text = tp_decode_escaped_range(start, tok_len, &text_len);

			if (is_prefix)
				tp_lowercase_inplace(text, text_len);

			tp_ensure_term_capacity(terms, capacity, *count);
			term = &(*terms)[*count];
			memset(term, 0, sizeof(TpQueryTerm));
			term->kind		 = is_prefix ? TP_QTERM_PREFIX : TP_QTERM_TERM;
			term->text		 = text;
			term->len		 = text_len;
			term->field_name = field_name ? field_name
										  : (inherited_field
													 ? pstrdup(inherited_field)
													 : NULL);
			(*count)++;
			p = end;
		}
	}

	if (in_group)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unterminated field group in query"),
				 errhint("Add a closing ')' to terminate the field group.")));

	*pp = p;
}

TpParsedQuery *
tp_parse_query(const char *query_text)
{
	TpParsedQuery *pq;
	const char	  *p;
	int			   capacity = 8;
	int			   count	= 0;
	TpQueryTerm	  *terms;

	if (query_text == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("query text is NULL")));

	terms = palloc(capacity * sizeof(TpQueryTerm));

	p = query_text;
	tp_parse_query_clauses(&p, &terms, &capacity, &count, NULL, false);

	pq			   = palloc(sizeof(TpParsedQuery));
	pq->term_count = count;
	pq->terms	   = terms;
	return pq;
}

void
tp_free_parsed_query(TpParsedQuery *pq)
{
	int i;

	if (pq == NULL)
		return;
	for (i = 0; i < pq->term_count; i++)
	{
		if (pq->terms[i].text)
			pfree(pq->terms[i].text);
		if (pq->terms[i].field_name)
			pfree(pq->terms[i].field_name);
		if (pq->terms[i].phrase_tokens)
		{
			int j;
			for (j = 0; j < pq->terms[i].phrase_token_count; j++)
			{
				if (pq->terms[i].phrase_tokens[j])
					pfree(pq->terms[i].phrase_tokens[j]);
			}
			pfree(pq->terms[i].phrase_tokens);
		}
	}
	if (pq->terms)
		pfree(pq->terms);
	pfree(pq);
}

/* ------------------------------------------------------------------ */
/* Prefix expansion                                                   */
/* ------------------------------------------------------------------ */

/*
 * Insert a term into a sorted+deduplicated dynamic array.
 * Returns true if newly inserted, false if duplicate.
 * Caller passes ownership of 'term' on insert; on dup it pfrees.
 *
 * O(N) per insert with N = current size; bounded by max_expansions
 * (default 50) so this is cheap.
 */
static bool
tp_collected_terms_insert(
		char ***terms, int *count, int *capacity, char *term, int max_terms)
{
	int i, j;

	if (*count >= max_terms)
	{
		pfree(term);
		return false;
	}

	/* Linear scan dedupe (sorted insertion) */
	for (i = 0; i < *count; i++)
	{
		int cmp = strcmp((*terms)[i], term);
		if (cmp == 0)
		{
			pfree(term);
			return false;
		}
		if (cmp > 0)
			break;
	}

	if (*count >= *capacity)
	{
		*capacity = (*capacity == 0) ? 8 : (*capacity * 2);
		if (*capacity > max_terms)
			*capacity = max_terms;
		if (*terms == NULL)
			*terms = palloc(*capacity * sizeof(char *));
		else
			*terms = repalloc(*terms, *capacity * sizeof(char *));
	}

	/* Shift right from i */
	for (j = *count; j > i; j--)
		(*terms)[j] = (*terms)[j - 1];
	(*terms)[i] = term;
	(*count)++;
	return true;
}

int
tp_expand_prefix(
		TpLocalIndexState *state,
		Relation		   index,
		BlockNumber		  *level_heads,
		const char		  *prefix,
		int				   max_expansions,
		char			***out_terms)
{
	char **collected = NULL;
	int	   count	 = 0;
	int	   capacity	 = 0;
	int	   i;

	*out_terms = NULL;

	if (prefix == NULL || *prefix == '\0' || max_expansions <= 0)
		return 0;

	/*
	 * Collect from ALL sources before capping.  Use a generous
	 * internal limit so segment terms are not silently excluded
	 * when the memtable alone fills the user-facing cap.
	 */
	{
		int internal_cap = max_expansions * 4;
		if (internal_cap < 200)
			internal_cap = 200;

		/* Memtable */
		if (state != NULL)
		{
			char **mt_terms = NULL;
			int	   mt_count = 0;

			tp_memtable_collect_prefix_terms(
					state, prefix, internal_cap, &mt_terms, &mt_count);
			for (i = 0; i < mt_count; i++)
			{
				tp_collected_terms_insert(
						&collected,
						&count,
						&capacity,
						mt_terms[i],
						internal_cap);
			}
			if (mt_terms)
				pfree(mt_terms);
		}

		/* All segment levels */
		if (index != NULL && level_heads != NULL)
		{
			int level;
			for (level = 0; level < TP_MAX_LEVELS; level++)
			{
				char **seg_terms = NULL;
				int	   seg_count = 0;

				if (level_heads[level] == InvalidBlockNumber)
					continue;

				tp_segment_collect_prefix_terms(
						index,
						level_heads[level],
						prefix,
						internal_cap,
						&seg_terms,
						&seg_count);

				for (i = 0; i < seg_count; i++)
				{
					tp_collected_terms_insert(
							&collected,
							&count,
							&capacity,
							seg_terms[i],
							internal_cap);
				}
				if (seg_terms)
					pfree(seg_terms);
			}
		}
	}

	/*
	 * Truncate to caller's cap.  collected is sorted by
	 * tp_collected_terms_insert, so we keep the lexicographically
	 * first terms (deterministic across spill/merge).
	 */
	if (count > max_expansions)
	{
		for (i = max_expansions; i < count; i++)
			pfree(collected[i]);
		count = max_expansions;
	}

	*out_terms = collected;
	return count;
}

int
tp_expand_fuzzy(
		TpLocalIndexState *state,
		Relation		   index,
		BlockNumber		  *level_heads,
		const char		  *query_term,
		int				   max_distance,
		int				   max_expansions,
		bool			   prefix,
		TpFuzzyCandidate **out_candidates)
{
	TpFuzzyCandidate *collected = NULL;
	int				  count		= 0;
	int				  capacity	= 0;
	int				  i;

	*out_candidates = NULL;

	if (query_term == NULL || *query_term == '\0' || max_expansions <= 0)
		return 0;

	if (state != NULL)
	{
		TpFuzzyCandidate *mt_candidates = NULL;
		int				  mt_count		= 0;

		tp_memtable_collect_fuzzy_terms(
				state,
				query_term,
				max_distance,
				max_expansions,
				prefix,
				&mt_candidates,
				&mt_count);
		for (i = 0; i < mt_count; i++)
		{
			tp_fuzzy_candidates_insert(
					&collected,
					&count,
					&capacity,
					mt_candidates[i].term,
					mt_candidates[i].distance,
					max_expansions);
		}
		if (mt_candidates)
			pfree(mt_candidates);
	}

	if (index != NULL && level_heads != NULL)
	{
		int level;

		for (level = 0; level < TP_MAX_LEVELS; level++)
		{
			TpFuzzyCandidate *seg_candidates = NULL;
			int				  seg_count		 = 0;

			if (level_heads[level] == InvalidBlockNumber)
				continue;

			tp_segment_collect_fuzzy_terms(
					index,
					level_heads[level],
					query_term,
					max_distance,
					max_expansions,
					prefix,
					&seg_candidates,
					&seg_count);

			for (i = 0; i < seg_count; i++)
			{
				tp_fuzzy_candidates_insert(
						&collected,
						&count,
						&capacity,
						seg_candidates[i].term,
						seg_candidates[i].distance,
						max_expansions);
			}
			if (seg_candidates)
				pfree(seg_candidates);
		}
	}

	*out_candidates = collected;
	return count;
}

/*
 * Resolve a parsed query into a flat (term, query_freq) list ready
 * for BMW. Regular tokens are funneled through to_tsvector_byid using
 * the index's text_config (so stemming/stopwords match indexing);
 * prefix tokens are dictionary-expanded.
 */
/*
 * Build a single stemmed token by running `token_text` through
 * to_tsvector_byid. Returns palloc'd cstring; empty string for
 * stopwords.
 */
static char *
resolve_stem_one(const char *token_text, Oid text_config_oid)
{
	Datum	   tsv_datum;
	TSVector   tsv;
	WordEntry *entries;
	char	  *lex_start;
	char	  *result;

	if (!OidIsValid(text_config_oid))
		return pstrdup(token_text);

	tsv_datum = DirectFunctionCall2(
			to_tsvector_byid,
			ObjectIdGetDatum(text_config_oid),
			PointerGetDatum(cstring_to_text(token_text)));
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

/*
 * Allocate a new palloc'd cstring equal to tag_byte || stem,
 * taking ownership of `stem` (pfree'd here). If tag_byte == 0
 * (single-col path), returns stem unchanged.
 */
static char *
resolve_apply_tag(char *stem, unsigned char tag_byte)
{
	size_t len;
	char  *tagged;

	if (tag_byte == 0)
		return stem;

	len		  = strlen(stem);
	tagged	  = palloc(len + 2);
	tagged[0] = (char)tag_byte;
	memcpy(tagged + 1, stem, len + 1);
	pfree(stem);
	return tagged;
}

static void
tp_append_resolved_term(
		char  ***terms,
		int32  **freqs,
		float4 **weights,
		int		*count,
		int		*capacity,
		char	*term,
		float4	 weight)
{
	if (*count >= *capacity)
	{
		*capacity = (*capacity == 0) ? 8 : *capacity * 2;
		*terms	  = repalloc(*terms, *capacity * sizeof(char *));
		*freqs	  = repalloc(*freqs, *capacity * sizeof(int32));
		if (weights != NULL && *weights != NULL)
			*weights = repalloc(*weights, *capacity * sizeof(float4));
	}

	(*terms)[*count] = term;
	(*freqs)[*count] = 1;
	if (weights != NULL && *weights != NULL)
		(*weights)[*count] = weight;
	(*count)++;
}

int
tp_resolve_query_terms_ex(
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
		float4							**out_weights)
{
	int	   i;
	int	   count	  = 0;
	int	   capacity	  = 0;
	char **terms	  = NULL;
	int32 *freqs	  = NULL;
	bool   multi_col  = (metap != NULL && metap->num_fields > 1);
	int	   num_fields = multi_col ? metap->num_fields : 1;

	*out_terms = NULL;
	*out_freqs = NULL;
	if (out_weights != NULL)
		*out_weights = NULL;

	if (pq == NULL || pq->term_count == 0)
		return 0;

	/*
	 * Worst-case upper bound:
	 *   each TERM clause emits num_fields resolved terms
	 *   each PREFIX clause emits num_fields × max_prefix_expansions
	 *   each PHRASE clause emits num_fields × phrase_token_count
	 *   each PHRASE_PREFIX: num_fields × (phrase_token_count - 1 + max_exp)
	 */
	capacity = 0;
	for (i = 0; i < pq->term_count; i++)
	{
		const TpQueryTerm *qt = &pq->terms[i];
		int per_clause_fields = (qt->field_name != NULL) ? 1 : num_fields;
		switch (qt->kind)
		{
		case TP_QTERM_TERM:
			capacity += per_clause_fields *
						(fuzzy ? tp_max_fuzzy_expansions : 1);
			break;
		case TP_QTERM_PREFIX:
			capacity += per_clause_fields * (fuzzy ? tp_max_fuzzy_expansions
												   : tp_max_prefix_expansions);
			break;
		case TP_QTERM_PHRASE:
			if (fuzzy)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("fuzzy phrase queries are not supported yet"),
						 errhint("Use bm25_fuzzy() with unquoted terms, or "
								 "use to_bm25query() for exact phrases.")));
			capacity += per_clause_fields * qt->phrase_token_count;
			break;
		case TP_QTERM_PHRASE_PREFIX:
			if (fuzzy)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("fuzzy phrase-prefix queries are not "
								"supported "
								"yet"),
						 errhint("Use bm25_fuzzy() with an unquoted prefix, "
								 "or "
								 "use to_bm25query() for exact phrase-prefix "
								 "queries.")));
			capacity += per_clause_fields * (qt->phrase_token_count - 1 +
											 tp_max_prefix_expansions);
			break;
		}
	}
	if (capacity == 0)
		capacity = 1;

	terms = palloc(capacity * sizeof(char *));
	freqs = palloc(capacity * sizeof(int32));
	if (out_weights != NULL)
		*out_weights = palloc(capacity * sizeof(float4));

	for (i = 0; i < pq->term_count; i++)
	{
		const TpQueryTerm *qt = &pq->terms[i];
		int				   field_start;
		int				   field_end;
		int				   f;

		/*
		 * Determine the field range this clause targets. Field-
		 * qualified clauses target one field; unqualified in multi-
		 * col expand to all; single-col uses field_start=0 sentinel
		 * with tag=0.
		 */
		if (qt->field_name != NULL)
		{
			int fidx;
			if (!multi_col)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("field-scoped query '%s:' has no matching "
								"field in single-column index",
								qt->field_name)));
			fidx = tp_metapage_find_field(metap, qt->field_name);
			if (fidx < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("field \"%s\" not in index field list",
								qt->field_name)));
			field_start = fidx;
			field_end	= fidx + 1;
		}
		else
		{
			field_start = 0;
			field_end	= num_fields;
		}

		for (f = field_start; f < field_end; f++)
		{
			unsigned char tag = multi_col ? tp_field_tag_byte(f) : 0;

			switch (qt->kind)
			{
			case TP_QTERM_TERM:
			{
				char *stem = resolve_stem_one(qt->text, text_config_oid);
				if (stem[0] == '\0')
				{
					pfree(stem);
					break;
				}
				stem = resolve_apply_tag(stem, tag);
				if (fuzzy &&
					tp_fuzzy_lexical_char_len(stem) >= tp_min_fuzzy_term_len)
				{
					TpFuzzyCandidate *expansions = NULL;
					int				  exp_count;
					int				  j;

					exp_count = tp_expand_fuzzy(
							state,
							index,
							level_heads,
							stem,
							fuzzy_max_distance,
							tp_max_fuzzy_expansions,
							false,
							&expansions);
					pfree(stem);

					for (j = 0; j < exp_count; j++)
					{
						tp_append_resolved_term(
								&terms,
								&freqs,
								out_weights,
								&count,
								&capacity,
								expansions[j].term,
								tp_fuzzy_distance_weight(
										expansions[j].distance));
					}
					if (expansions)
						pfree(expansions);
				}
				else
				{
					tp_append_resolved_term(
							&terms,
							&freqs,
							out_weights,
							&count,
							&capacity,
							stem,
							1.0f);
				}
				break;
			}

			case TP_QTERM_PREFIX:
			{
				char  *tagged_prefix;
				char **expansions = NULL;
				int	   exp_count;
				int	   j;

				{
					size_t plen = strlen(qt->text);
					if (tag == 0)
					{
						tagged_prefix = pstrdup(qt->text);
					}
					else
					{
						tagged_prefix	 = palloc(plen + 2);
						tagged_prefix[0] = (char)tag;
						memcpy(tagged_prefix + 1, qt->text, plen + 1);
					}
				}

				if (fuzzy && tp_fuzzy_lexical_char_len(tagged_prefix) >=
									 tp_min_fuzzy_term_len)
				{
					TpFuzzyCandidate *fuzzy_expansions = NULL;

					exp_count = tp_expand_fuzzy(
							state,
							index,
							level_heads,
							tagged_prefix,
							fuzzy_max_distance,
							tp_max_fuzzy_expansions,
							true,
							&fuzzy_expansions);
					for (j = 0; j < exp_count; j++)
					{
						tp_append_resolved_term(
								&terms,
								&freqs,
								out_weights,
								&count,
								&capacity,
								fuzzy_expansions[j].term,
								tp_fuzzy_distance_weight(
										fuzzy_expansions[j].distance));
					}
					if (fuzzy_expansions)
						pfree(fuzzy_expansions);
					pfree(tagged_prefix);
					break;
				}

				exp_count = tp_expand_prefix(
						state,
						index,
						level_heads,
						tagged_prefix,
						tp_max_prefix_expansions,
						&expansions);
				pfree(tagged_prefix);

				for (j = 0; j < exp_count && count < capacity; j++)
				{
					tp_append_resolved_term(
							&terms,
							&freqs,
							out_weights,
							&count,
							&capacity,
							expansions[j],
							1.0f);
				}
				if (expansions)
					pfree(expansions);
				break;
			}

			case TP_QTERM_PHRASE:
			{
				int j;
				for (j = 0; j < qt->phrase_token_count; j++)
				{
					char *stem = resolve_stem_one(
							qt->phrase_tokens[j], text_config_oid);
					if (stem[0] == '\0')
					{
						pfree(stem);
						continue;
					}
					stem = resolve_apply_tag(stem, tag);
					tp_append_resolved_term(
							&terms,
							&freqs,
							out_weights,
							&count,
							&capacity,
							stem,
							1.0f);
				}
				break;
			}

			case TP_QTERM_PHRASE_PREFIX:
			{
				int j;
				/* Non-last tokens: stem + tag */
				for (j = 0; j < qt->phrase_token_count - 1; j++)
				{
					char *stem = resolve_stem_one(
							qt->phrase_tokens[j], text_config_oid);
					if (stem[0] == '\0')
					{
						pfree(stem);
						continue;
					}
					stem = resolve_apply_tag(stem, tag);
					if (count < capacity)
					{
						terms[count] = stem;
						freqs[count] = 1;
						count++;
					}
					else
						pfree(stem);
				}
				/* Last token: prefix-expand under field tag */
				if (qt->phrase_token_count > 0)
				{
					const char *prefix_raw =
							qt->phrase_tokens[qt->phrase_token_count - 1];
					char  *tagged_prefix;
					char **expansions = NULL;
					int	   exp_count;
					int	   k;
					size_t plen = strlen(prefix_raw);

					if (tag == 0)
					{
						tagged_prefix = pstrdup(prefix_raw);
					}
					else
					{
						tagged_prefix	 = palloc(plen + 2);
						tagged_prefix[0] = (char)tag;
						memcpy(tagged_prefix + 1, prefix_raw, plen + 1);
					}

					exp_count = tp_expand_prefix(
							state,
							index,
							level_heads,
							tagged_prefix,
							tp_max_prefix_expansions,
							&expansions);
					pfree(tagged_prefix);

					for (k = 0; k < exp_count; k++)
						tp_append_resolved_term(
								&terms,
								&freqs,
								out_weights,
								&count,
								&capacity,
								expansions[k],
								1.0f);
					if (expansions)
						pfree(expansions);
				}
				break;
			}
			}
		}
	}

	*out_terms = terms;
	*out_freqs = freqs;
	return count;
}

int
tp_resolve_query_terms(
		TpParsedQuery					 *pq,
		TpLocalIndexState				 *state,
		Relation						  index,
		BlockNumber						 *level_heads,
		Oid								  text_config_oid,
		const struct TpIndexMetaPageData *metap,
		char						   ***out_terms,
		int32							**out_freqs)
{
	return tp_resolve_query_terms_ex(
			pq,
			state,
			index,
			level_heads,
			text_config_oid,
			metap,
			false,
			0,
			out_terms,
			out_freqs,
			NULL);
}
