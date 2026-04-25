/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * highlight.c - BM25 snippets and byte-offset highlight ranges
 */
#include <postgres.h>

#include <access/genam.h>
#include <access/relation.h>
#include <catalog/pg_type_d.h>
#include <ctype.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <mb/pg_wchar.h>
#include <tsearch/ts_type.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/lsyscache.h>
#include <utils/rangetypes.h>
#include <utils/rel.h>
#include <utils/typcache.h>
#include <varatt.h>

#include "constants.h"
#include "highlight/highlight.h"
#include "index/metapage.h"
#include "index/state.h"
#include "types/markup.h"
#include "types/query.h"
#include "types/query_parser.h"

#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif
#ifndef Max
#define Max(x, y) ((x) > (y) ? (x) : (y))
#endif

typedef enum TpHighlightMatchType
{
	TP_HL_EXACT,
	TP_HL_PREFIX,
	TP_HL_PHRASE,
	TP_HL_PHRASE_PREFIX
} TpHighlightMatchType;

typedef struct TpHighlightRange
{
	int					 start;
	int					 end;
	char				*term;
	char				*matched_text;
	TpHighlightMatchType type;
} TpHighlightRange;

typedef struct TpHighlightRanges
{
	TpHighlightRange *items;
	int				  count;
	int				  capacity;
} TpHighlightRanges;

typedef struct TpHighlightClause
{
	TpQueryTermKind kind;
	char		  **terms;
	int				term_count;
	int				field_idx;
} TpHighlightClause;

typedef struct TpHighlightPlan
{
	TpHighlightClause *clauses;
	int				   count;
	int				   capacity;
} TpHighlightPlan;

typedef struct TpDocToken
{
	int	  start;
	int	  end;
	int	  pos;
	char *term;
} TpDocToken;

PG_FUNCTION_INFO_V1(bm25_snippet);
PG_FUNCTION_INFO_V1(bm25_snippet_text);
PG_FUNCTION_INFO_V1(bm25_snippet_positions);
PG_FUNCTION_INFO_V1(bm25_snippet_positions_text);
PG_FUNCTION_INFO_V1(bm25_highlights);

static const char *
tp_hl_match_type_name(TpHighlightMatchType type)
{
	switch (type)
	{
	case TP_HL_EXACT:
		return "exact";
	case TP_HL_PREFIX:
		return "prefix";
	case TP_HL_PHRASE:
		return "phrase";
	case TP_HL_PHRASE_PREFIX:
		return "phrase_prefix";
	}
	return "exact";
}

static void
tp_hl_ranges_init(TpHighlightRanges *ranges)
{
	ranges->count	 = 0;
	ranges->capacity = 8;
	ranges->items	 = palloc0(ranges->capacity * sizeof(TpHighlightRange));
}

static void
tp_hl_ranges_append(
		TpHighlightRanges	*ranges,
		int					 start,
		int					 end,
		const char			*term,
		const char			*doc,
		TpHighlightMatchType type)
{
	TpHighlightRange *range;

	if (start < 0 || end <= start)
		return;

	if (ranges->count >= ranges->capacity)
	{
		ranges->capacity *= 2;
		ranges->items = repalloc(
				ranges->items, ranges->capacity * sizeof(TpHighlightRange));
	}

	range				= &ranges->items[ranges->count++];
	range->start		= start;
	range->end			= end;
	range->term			= pstrdup(term ? term : "");
	range->matched_text = pnstrdup(doc + start, end - start);
	range->type			= type;
}

static int
tp_hl_range_cmp(const void *a, const void *b)
{
	const TpHighlightRange *ra = (const TpHighlightRange *)a;
	const TpHighlightRange *rb = (const TpHighlightRange *)b;

	if (ra->start != rb->start)
		return (ra->start < rb->start) ? -1 : 1;
	if (ra->end != rb->end)
		return (ra->end < rb->end) ? -1 : 1;
	return 0;
}

static void
tp_hl_ranges_sort_unique(TpHighlightRanges *ranges)
{
	int read_i;
	int write_i;

	if (ranges->count <= 1)
		return;

	qsort(ranges->items,
		  ranges->count,
		  sizeof(TpHighlightRange),
		  tp_hl_range_cmp);

	write_i = 0;
	for (read_i = 1; read_i < ranges->count; read_i++)
	{
		TpHighlightRange *prev = &ranges->items[write_i];
		TpHighlightRange *cur  = &ranges->items[read_i];

		if (cur->start == prev->start && cur->end == prev->end &&
			strcmp(cur->term, prev->term) == 0 && cur->type == prev->type)
			continue;

		ranges->items[++write_i] = *cur;
	}
	ranges->count = write_i + 1;
}

static char *
tp_hl_stem_one(const char *token, Oid text_config_oid)
{
	Datum	   tsv_datum;
	TSVector   tsv;
	WordEntry *entries;
	char	  *lex_start;
	char	  *result;

	if (!OidIsValid(text_config_oid))
		return pstrdup(token);

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

static void
tp_hl_plan_append_clause(
		TpHighlightPlan *plan,
		TpQueryTermKind	 kind,
		char		   **terms,
		int				 term_count,
		int				 field_idx)
{
	TpHighlightClause *clause;

	if (term_count <= 0)
		return;

	if (plan->count >= plan->capacity)
	{
		plan->capacity *= 2;
		plan->clauses = repalloc(
				plan->clauses, plan->capacity * sizeof(TpHighlightClause));
	}

	clause			   = &plan->clauses[plan->count++];
	clause->kind	   = kind;
	clause->terms	   = terms;
	clause->term_count = term_count;
	clause->field_idx  = field_idx;
}

static bool
tp_hl_clause_targets_field(
		TpQueryTerm				  *qt,
		const TpIndexMetaPageData *metap,
		int						   target_field_idx,
		int						  *out_field_idx)
{
	if (out_field_idx)
		*out_field_idx = -1;

	if (qt->field_name == NULL || metap == NULL || metap->num_fields <= 1)
		return true;

	{
		int field_idx = tp_metapage_find_field(metap, qt->field_name);
		if (field_idx < 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("field \"%s\" not in index field list",
							qt->field_name)));

		if (out_field_idx)
			*out_field_idx = field_idx;

		if (target_field_idx >= 0 && target_field_idx != field_idx)
			return false;
	}

	return true;
}

static TpHighlightPlan *
tp_hl_build_plan(
		TpParsedQuery			  *pq,
		TpLocalIndexState		  *state,
		Relation				   index_rel,
		BlockNumber				  *level_heads,
		Oid						   text_config_oid,
		const TpIndexMetaPageData *metap,
		int						   target_field_idx)
{
	TpHighlightPlan *plan = palloc0(sizeof(TpHighlightPlan));
	int				 i;

	plan->capacity = 8;
	plan->clauses  = palloc0(plan->capacity * sizeof(TpHighlightClause));

	for (i = 0; i < pq->term_count; i++)
	{
		TpQueryTerm *qt		   = &pq->terms[i];
		int			 field_idx = -1;

		if (!tp_hl_clause_targets_field(
					qt, metap, target_field_idx, &field_idx))
			continue;

		switch (qt->kind)
		{
		case TP_QTERM_TERM:
		{
			char **terms = palloc0(sizeof(char *));
			terms[0]	 = tp_hl_stem_one(qt->text, text_config_oid);
			if (terms[0][0] == '\0')
			{
				pfree(terms[0]);
				pfree(terms);
				break;
			}
			tp_hl_plan_append_clause(plan, qt->kind, terms, 1, field_idx);
			break;
		}
		case TP_QTERM_PREFIX:
		{
			char **terms	  = NULL;
			int	   term_count = tp_expand_prefix(
					   state,
					   index_rel,
					   level_heads,
					   qt->text,
					   tp_max_prefix_expansions,
					   &terms);

			if (term_count == 0)
			{
				terms	   = palloc0(sizeof(char *));
				terms[0]   = pstrdup(qt->text);
				term_count = 1;
			}
			tp_hl_plan_append_clause(
					plan, qt->kind, terms, term_count, field_idx);
			break;
		}
		case TP_QTERM_PHRASE:
		case TP_QTERM_PHRASE_PREFIX:
		{
			int	   j;
			char **terms = palloc0(qt->phrase_token_count * sizeof(char *));
			bool   skip	 = false;

			for (j = 0; j < qt->phrase_token_count; j++)
			{
				bool last_prefix =
						(qt->kind == TP_QTERM_PHRASE_PREFIX &&
						 j == qt->phrase_token_count - 1);
				terms[j] = last_prefix ? pstrdup(qt->phrase_tokens[j])
									   : tp_hl_stem_one(
												 qt->phrase_tokens[j],
												 text_config_oid);
				if (!last_prefix && terms[j][0] == '\0')
					skip = true;
			}

			if (skip)
			{
				for (j = 0; j < qt->phrase_token_count; j++)
					pfree(terms[j]);
				pfree(terms);
				break;
			}

			tp_hl_plan_append_clause(
					plan, qt->kind, terms, qt->phrase_token_count, field_idx);
			break;
		}
		}
	}

	return plan;
}

static bool
tp_hl_token_matches_term(TpDocToken *token, const char *term, bool prefix)
{
	if (token->term == NULL)
		return false;
	if (prefix)
		return strncmp(token->term, term, strlen(term)) == 0;
	return strcmp(token->term, term) == 0;
}

static inline bool
tp_hl_is_token_byte(unsigned char c)
{
	return isalnum(c) || c == '_' || c >= 0x80;
}

static TpDocToken *
tp_hl_parse_doc_tokens(
		const char *doc, int doc_len, Oid text_config_oid, int *out_count)
{
	TpDocToken *tokens;
	int			capacity = 16;
	int			count	 = 0;
	int			pos		 = 1;
	int			i		 = 0;

	tokens = palloc0(capacity * sizeof(TpDocToken));
	while (i < doc_len)
	{
		int	  start;
		int	  end;
		char *raw;
		char *stem;

		while (i < doc_len && !tp_hl_is_token_byte((unsigned char)doc[i]))
			i++;
		if (i >= doc_len)
			break;

		start = i;
		while (i < doc_len && tp_hl_is_token_byte((unsigned char)doc[i]))
		{
			i += (unsigned char)doc[i] >= 0x80 ? pg_mblen(doc + i) : 1;
		}
		end = i;

		raw	 = pnstrdup(doc + start, end - start);
		stem = tp_hl_stem_one(raw, text_config_oid);
		pfree(raw);
		if (stem[0] == '\0')
		{
			pfree(stem);
			continue;
		}

		if (count >= capacity)
		{
			capacity *= 2;
			tokens = repalloc(tokens, capacity * sizeof(TpDocToken));
		}
		tokens[count].start = start;
		tokens[count].end	= end;
		tokens[count].pos	= pos++;
		tokens[count].term	= stem;
		count++;
	}

	*out_count = count;
	return tokens;
}

static void
tp_hl_collect_ranges_from_tokens(
		TpHighlightPlan	  *plan,
		const char		  *doc,
		TpDocToken		  *tokens,
		int				   token_count,
		TpHighlightRanges *ranges)
{
	int i, j;

	for (i = 0; i < plan->count; i++)
	{
		TpHighlightClause *clause = &plan->clauses[i];

		switch (clause->kind)
		{
		case TP_QTERM_TERM:
		case TP_QTERM_PREFIX:
		{
			bool is_prefix = (clause->kind == TP_QTERM_PREFIX);
			int	 t;

			for (t = 0; t < token_count; t++)
			{
				for (j = 0; j < clause->term_count; j++)
				{
					if (tp_hl_token_matches_term(
								&tokens[t], clause->terms[j], is_prefix))
					{
						tp_hl_ranges_append(
								ranges,
								tokens[t].start,
								tokens[t].end,
								clause->terms[j],
								doc,
								is_prefix ? TP_HL_PREFIX : TP_HL_EXACT);
						break;
					}
				}
			}
			break;
		}
		case TP_QTERM_PHRASE:
		case TP_QTERM_PHRASE_PREFIX:
		{
			bool last_is_prefix = (clause->kind == TP_QTERM_PHRASE_PREFIX);
			int	 t;

			for (t = 0; t <= token_count - clause->term_count; t++)
			{
				bool ok = true;
				int	 k;

				for (k = 0; k < clause->term_count; k++)
				{
					bool is_prefix = last_is_prefix &&
									 k == clause->term_count - 1;

					if (k > 0 && tokens[t + k].pos != tokens[t].pos + k)
					{
						ok = false;
						break;
					}
					if (!tp_hl_token_matches_term(
								&tokens[t + k], clause->terms[k], is_prefix))
					{
						ok = false;
						break;
					}
				}

				if (ok)
				{
					StringInfoData phrase;

					initStringInfo(&phrase);
					for (k = 0; k < clause->term_count; k++)
					{
						if (k > 0)
							appendStringInfoChar(&phrase, ' ');
						appendStringInfoString(&phrase, clause->terms[k]);
					}
					tp_hl_ranges_append(
							ranges,
							tokens[t].start,
							tokens[t + clause->term_count - 1].end,
							phrase.data,
							doc,
							last_is_prefix ? TP_HL_PHRASE_PREFIX
										   : TP_HL_PHRASE);
					pfree(phrase.data);
				}
			}
			break;
		}
		}
	}

	tp_hl_ranges_sort_unique(ranges);
}

static int
tp_hl_resolve_target_field(
		const TpIndexMetaPageData *metap, const char *field_name)
{
	int field_idx;

	if (field_name == NULL)
		return -1;

	if (metap->num_fields <= 1)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("field \"%s\" not in single-column index",
						field_name)));

	field_idx = tp_metapage_find_field(metap, field_name);
	if (field_idx < 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("field \"%s\" not in index field list", field_name)));

	return field_idx;
}

static Relation
tp_hl_open_index(TpQuery *query, const char *index_name, Oid *index_oid_out)
{
	Oid index_oid = InvalidOid;

	if (index_name != NULL)
	{
		TpQuery *named =
				create_tpquery_from_name(get_tpquery_text(query), index_name);
		index_oid = get_tpquery_index_oid(named);
	}
	else
	{
		index_oid = get_tpquery_index_oid(query);
	}

	if (!OidIsValid(index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("highlighting requires a BM25 index"),
				 errhint("Pass to_bm25query(query, index_name) or the "
						 "index_name argument.")));

	if (index_oid_out)
		*index_oid_out = index_oid;
	return index_open(index_oid, AccessShareLock);
}

static void
tp_hl_collect(
		text			  *document,
		TpQuery			  *query,
		const char		  *index_name,
		const char		  *field_name,
		TpHighlightRanges *ranges,
		text			 **normalized_out)
{
	char			  *query_text = get_tpquery_text(query);
	Relation		   index_rel  = NULL;
	TpIndexMetaPage	   metap	  = NULL;
	TpLocalIndexState *state	  = NULL;
	Oid				   index_oid;
	Oid				   text_config_oid;
	BlockNumber		   level_heads[TP_MAX_LEVELS];
	TpParsedQuery	  *pq	   = NULL;
	TpHighlightPlan	  *plan	   = NULL;
	char			  *doc	   = text_to_cstring(document);
	int				   doc_len = VARSIZE_ANY_EXHDR(document);
	int				   target_field_idx;
	TpDocToken		  *tokens;
	int				   token_count;
	int				   i;

	if (normalized_out)
		*normalized_out = document;

	tp_hl_ranges_init(ranges);

	index_rel = tp_hl_open_index(query, index_name, &index_oid);

	PG_TRY();
	{
		metap			= tp_get_metapage(index_rel);
		text_config_oid = metap->text_config_oid;
		for (i = 0; i < TP_MAX_LEVELS; i++)
			level_heads[i] = metap->level_heads[i];

		state = tp_get_local_index_state(index_oid);
		if (!state)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not get index state for index OID %u",
							index_oid)));

		target_field_idx = tp_hl_resolve_target_field(metap, field_name);
		pq				 = tp_parse_query(query_text);
		plan			 = tp_hl_build_plan(
				pq,
				state,
				index_rel,
				level_heads,
				text_config_oid,
				metap,
				target_field_idx);
		if (plan->count > 0)
		{
			int	  fmt_idx = (target_field_idx >= 0) ? target_field_idx : 0;
			uint8 fmt	  = tp_metapage_field_format(metap, fmt_idx);
			if (fmt != TP_FORMAT_PLAIN)
			{
				text *normalized =
						tp_normalize_markup(document, (TpContentFormat)fmt);
				doc		= text_to_cstring(normalized);
				doc_len = VARSIZE_ANY_EXHDR(normalized);
				if (normalized_out)
					*normalized_out = normalized;
			}
			tokens = tp_hl_parse_doc_tokens(
					doc, doc_len, text_config_oid, &token_count);
			tp_hl_collect_ranges_from_tokens(
					plan, doc, tokens, token_count, ranges);
		}

		tp_free_parsed_query(pq);
		pfree(metap);
		metap = NULL;
		index_close(index_rel, AccessShareLock);
		index_rel = NULL;
	}
	PG_CATCH();
	{
		if (pq)
			tp_free_parsed_query(pq);
		if (metap)
			pfree(metap);
		if (index_rel)
			index_close(index_rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static int
tp_hl_effective_count(TpHighlightRanges *ranges, int limit, int offset)
{
	if (offset >= ranges->count)
		return 0;
	if (limit < 0 || offset + limit > ranges->count)
		return ranges->count - offset;
	return limit;
}

static RangeType *
tp_hl_make_int4range(int start, int end)
{
	TypeCacheEntry *typcache;
	RangeBound		lower;
	RangeBound		upper;

	typcache = lookup_type_cache(INT4RANGEOID, TYPECACHE_RANGE_INFO);

	lower.val		= Int32GetDatum(start);
	lower.infinite	= false;
	lower.inclusive = true;
	lower.lower		= true;

	upper.val		= Int32GetDatum(end);
	upper.infinite	= false;
	upper.inclusive = false;
	upper.lower		= false;

	return range_serialize(typcache, &lower, &upper, false, NULL);
}

static ArrayType *
tp_hl_ranges_to_array(TpHighlightRanges *ranges, int limit, int offset)
{
	Datum *datums;
	int	   nitems;
	int	   i;
	int16  typlen;
	bool   typbyval;
	char   typalign;

	nitems = tp_hl_effective_count(ranges, limit, offset);
	if (nitems == 0)
		return construct_empty_array(INT4RANGEOID);

	datums = palloc0(nitems * sizeof(Datum));
	for (i = 0; i < nitems; i++)
	{
		TpHighlightRange *range = &ranges->items[offset + i];
		datums[i]				= RangeTypePGetDatum(
				  tp_hl_make_int4range(range->start, range->end));
	}

	get_typlenbyvalalign(INT4RANGEOID, &typlen, &typbyval, &typalign);
	return construct_array(
			datums, nitems, INT4RANGEOID, typlen, typbyval, typalign);
}

static bool
tp_hl_is_char_boundary(const char *doc, int len, int pos)
{
	int i = 0;

	if (pos <= 0 || pos >= len)
		return true;

	while (i < len)
	{
		if (i == pos)
			return true;
		i += pg_mblen(doc + i);
		if (i > pos)
			return false;
	}
	return false;
}

static int
tp_hl_prev_char_boundary(const char *doc, int len, int pos)
{
	while (pos > 0 && !tp_hl_is_char_boundary(doc, len, pos))
		pos--;
	return pos;
}

static int
tp_hl_next_char_boundary(const char *doc, int len, int pos)
{
	while (pos < len && !tp_hl_is_char_boundary(doc, len, pos))
		pos++;
	return pos;
}

static text *
tp_hl_render_snippet(
		text			  *document,
		TpHighlightRanges *ranges,
		const char		  *start_tag,
		const char		  *end_tag,
		int				   max_num_chars,
		int				   limit,
		int				   offset)
{
	char		  *doc = text_to_cstring(document);
	int			   len = VARSIZE_ANY_EXHDR(document);
	int			   nitems;
	int			   frag_start = 0;
	int			   frag_end	  = len;
	int			   cursor;
	int			   i;
	StringInfoData out;

	if (max_num_chars <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_num_chars must be greater than zero")));

	nitems = tp_hl_effective_count(ranges, limit, offset);
	if (nitems == 0)
		return cstring_to_text("");

	if (len > max_num_chars)
	{
		TpHighlightRange *first = &ranges->items[offset];
		int				  context =
				Max((max_num_chars - (first->end - first->start)) / 2, 0);

		frag_start = Max(first->start - context, 0);
		frag_end   = Min(frag_start + max_num_chars, len);
		if (frag_end - frag_start < max_num_chars)
			frag_start = Max(frag_end - max_num_chars, 0);
		frag_start = tp_hl_prev_char_boundary(doc, len, frag_start);
		frag_end   = tp_hl_next_char_boundary(doc, len, frag_end);
	}

	initStringInfo(&out);
	cursor = frag_start;
	for (i = 0; i < nitems; i++)
	{
		TpHighlightRange *range = &ranges->items[offset + i];
		int				  start = Max(range->start, frag_start);
		int				  end	= Min(range->end, frag_end);

		if (end <= frag_start || start >= frag_end)
			continue;
		if (start < cursor)
			start = cursor;
		if (start > cursor)
			appendBinaryStringInfo(&out, doc + cursor, start - cursor);
		appendStringInfoString(&out, start_tag);
		appendBinaryStringInfo(&out, doc + start, end - start);
		appendStringInfoString(&out, end_tag);
		cursor = end;
	}

	if (cursor < frag_end)
		appendBinaryStringInfo(&out, doc + cursor, frag_end - cursor);

	return cstring_to_text(out.data);
}

static char *
tp_hl_optional_text_arg(FunctionCallInfo fcinfo, int argno)
{
	if (PG_NARGS() <= argno || PG_ARGISNULL(argno))
		return NULL;
	return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

static int
tp_hl_optional_int_arg(FunctionCallInfo fcinfo, int argno, int default_value)
{
	if (PG_NARGS() <= argno || PG_ARGISNULL(argno))
		return default_value;
	return PG_GETARG_INT32(argno);
}

Datum
bm25_snippet(PG_FUNCTION_ARGS)
{
	text			 *document;
	TpQuery			 *query;
	char			 *index_name;
	char			 *start_tag;
	char			 *end_tag;
	char			 *field_name;
	int				  max_num_chars;
	int				  limit;
	int				  offset;
	TpHighlightRanges ranges;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();

	document	  = PG_GETARG_TEXT_PP(0);
	query		  = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
	index_name	  = tp_hl_optional_text_arg(fcinfo, 2);
	start_tag	  = tp_hl_optional_text_arg(fcinfo, 3);
	end_tag		  = tp_hl_optional_text_arg(fcinfo, 4);
	max_num_chars = tp_hl_optional_int_arg(fcinfo, 5, 150);
	limit		  = tp_hl_optional_int_arg(fcinfo, 6, -1);
	offset		  = tp_hl_optional_int_arg(fcinfo, 7, 0);
	field_name	  = tp_hl_optional_text_arg(fcinfo, 8);

	if (start_tag == NULL)
		start_tag = "<b>";
	if (end_tag == NULL)
		end_tag = "</b>";
	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("offset must be greater than or equal to zero")));

	{
		text *hl_doc = document;
		tp_hl_collect(
				document, query, index_name, field_name, &ranges, &hl_doc);
		PG_RETURN_TEXT_P(tp_hl_render_snippet(
				hl_doc,
				&ranges,
				start_tag,
				end_tag,
				max_num_chars,
				limit,
				offset));
	}
}

Datum
bm25_snippet_text(PG_FUNCTION_ARGS)
{
	text			 *document;
	char			 *query_cstr;
	char			 *index_name;
	char			 *start_tag;
	char			 *end_tag;
	char			 *field_name;
	int				  max_num_chars;
	int				  limit;
	int				  offset;
	TpQuery			 *query;
	TpHighlightRanges ranges;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	document	  = PG_GETARG_TEXT_PP(0);
	query_cstr	  = text_to_cstring(PG_GETARG_TEXT_PP(1));
	index_name	  = text_to_cstring(PG_GETARG_TEXT_PP(2));
	start_tag	  = tp_hl_optional_text_arg(fcinfo, 3);
	end_tag		  = tp_hl_optional_text_arg(fcinfo, 4);
	max_num_chars = tp_hl_optional_int_arg(fcinfo, 5, 150);
	limit		  = tp_hl_optional_int_arg(fcinfo, 6, -1);
	offset		  = tp_hl_optional_int_arg(fcinfo, 7, 0);
	field_name	  = tp_hl_optional_text_arg(fcinfo, 8);

	if (start_tag == NULL)
		start_tag = "<b>";
	if (end_tag == NULL)
		end_tag = "</b>";
	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("offset must be greater than or equal to zero")));

	query = create_tpquery_from_name(query_cstr, index_name);
	{
		text *hl_doc = document;
		tp_hl_collect(
				document, query, index_name, field_name, &ranges, &hl_doc);
		PG_RETURN_TEXT_P(tp_hl_render_snippet(
				hl_doc,
				&ranges,
				start_tag,
				end_tag,
				max_num_chars,
				limit,
				offset));
	}
}

Datum
bm25_snippet_positions(PG_FUNCTION_ARGS)
{
	text			 *document;
	TpQuery			 *query;
	char			 *index_name;
	char			 *field_name;
	int				  limit;
	int				  offset;
	TpHighlightRanges ranges;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	document   = PG_GETARG_TEXT_PP(0);
	query	   = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
	index_name = tp_hl_optional_text_arg(fcinfo, 2);
	limit	   = tp_hl_optional_int_arg(fcinfo, 3, -1);
	offset	   = tp_hl_optional_int_arg(fcinfo, 4, 0);
	field_name = tp_hl_optional_text_arg(fcinfo, 5);

	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("offset must be greater than or equal to zero")));

	tp_hl_collect(document, query, index_name, field_name, &ranges, NULL);
	PG_RETURN_ARRAYTYPE_P(tp_hl_ranges_to_array(&ranges, limit, offset));
}

Datum
bm25_snippet_positions_text(PG_FUNCTION_ARGS)
{
	text			 *document;
	char			 *query_cstr;
	char			 *index_name;
	char			 *field_name;
	int				  limit;
	int				  offset;
	TpQuery			 *query;
	TpHighlightRanges ranges;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	document   = PG_GETARG_TEXT_PP(0);
	query_cstr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	index_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
	limit	   = tp_hl_optional_int_arg(fcinfo, 3, -1);
	offset	   = tp_hl_optional_int_arg(fcinfo, 4, 0);
	field_name = tp_hl_optional_text_arg(fcinfo, 5);

	if (offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("offset must be greater than or equal to zero")));

	query = create_tpquery_from_name(query_cstr, index_name);
	tp_hl_collect(document, query, index_name, field_name, &ranges, NULL);
	PG_RETURN_ARRAYTYPE_P(tp_hl_ranges_to_array(&ranges, limit, offset));
}

static void
tp_hl_json_escape(StringInfo out, const char *str)
{
	const unsigned char *p = (const unsigned char *)str;

	appendStringInfoChar(out, '"');
	for (; *p; p++)
	{
		switch (*p)
		{
		case '"':
			appendStringInfoString(out, "\\\"");
			break;
		case '\\':
			appendStringInfoString(out, "\\\\");
			break;
		case '\b':
			appendStringInfoString(out, "\\b");
			break;
		case '\f':
			appendStringInfoString(out, "\\f");
			break;
		case '\n':
			appendStringInfoString(out, "\\n");
			break;
		case '\r':
			appendStringInfoString(out, "\\r");
			break;
		case '\t':
			appendStringInfoString(out, "\\t");
			break;
		default:
			if (*p < 0x20)
				appendStringInfo(out, "\\u%04x", *p);
			else
				appendStringInfoChar(out, *p);
			break;
		}
	}
	appendStringInfoChar(out, '"');
}

static void
tp_hl_append_json_field(
		StringInfo	out,
		const char *field_name,
		text	   *field_text,
		TpQuery	   *query,
		const char *index_name)
{
	TpHighlightRanges ranges;
	text			 *snippet;
	int				  i;

	{
		text *hl_doc = field_text;
		tp_hl_collect(
				field_text, query, index_name, field_name, &ranges, &hl_doc);
		snippet = tp_hl_render_snippet(
				hl_doc, &ranges, "<b>", "</b>", 150, -1, 0);
	}

	tp_hl_json_escape(out, field_name);
	appendStringInfoString(out, ":{\"snippet\":");
	tp_hl_json_escape(out, text_to_cstring(snippet));
	appendStringInfoString(out, ",\"positions\":[");
	for (i = 0; i < ranges.count; i++)
	{
		if (i > 0)
			appendStringInfoChar(out, ',');
		appendStringInfo(
				out, "[%d,%d]", ranges.items[i].start, ranges.items[i].end);
	}
	appendStringInfoString(out, "],\"matches\":[");
	for (i = 0; i < ranges.count; i++)
	{
		TpHighlightRange *range = &ranges.items[i];
		if (i > 0)
			appendStringInfoChar(out, ',');
		appendStringInfo(
				out,
				"{\"start\":%d,\"end\":%d,\"term\":",
				range->start,
				range->end);
		tp_hl_json_escape(out, range->term);
		appendStringInfoString(out, ",\"matched_text\":");
		tp_hl_json_escape(out, range->matched_text);
		appendStringInfoString(out, ",\"match_type\":");
		tp_hl_json_escape(out, tp_hl_match_type_name(range->type));
		appendStringInfoChar(out, '}');
	}
	appendStringInfoString(out, "]}");
}

Datum
bm25_highlights(PG_FUNCTION_ARGS)
{
	TpQuery		   *query;
	char		   *index_name;
	ArrayType	   *fields_array;
	Datum		   *field_datums;
	bool		   *field_nulls;
	int				field_count;
	Relation		index_rel = NULL;
	TpIndexMetaPage metap	  = NULL;
	StringInfoData	json;
	int				i;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	query		 = (TpQuery *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	index_name	 = text_to_cstring(PG_GETARG_TEXT_PP(1));
	fields_array = PG_GETARG_ARRAYTYPE_P(2);

	deconstruct_array(
			fields_array,
			TEXTOID,
			-1,
			false,
			TYPALIGN_INT,
			&field_datums,
			&field_nulls,
			&field_count);

	index_rel = tp_hl_open_index(query, index_name, NULL);
	PG_TRY();
	{
		metap = tp_get_metapage(index_rel);
		if (field_count != metap->num_fields)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("highlight field count does not match index"),
					 errdetail(
							 "Index has %d fields, but %d values were "
							 "provided.",
							 metap->num_fields,
							 field_count)));

		initStringInfo(&json);
		appendStringInfoChar(&json, '{');
		for (i = 0; i < field_count; i++)
		{
			const char *field_name = tp_metapage_field_name(metap, i);
			if (i > 0)
				appendStringInfoChar(&json, ',');
			if (field_nulls[i])
			{
				tp_hl_json_escape(&json, field_name);
				appendStringInfoString(
						&json,
						":{\"snippet\":null,\"positions\":[],\"matches\":[]}");
			}
			else
			{
				tp_hl_append_json_field(
						&json,
						field_name,
						DatumGetTextPP(field_datums[i]),
						query,
						index_name);
			}
		}
		appendStringInfoChar(&json, '}');

		pfree(metap);
		metap = NULL;
		index_close(index_rel, AccessShareLock);
		index_rel = NULL;
	}
	PG_CATCH();
	{
		if (metap)
			pfree(metap);
		if (index_rel)
			index_close(index_rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data)));
}
