/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * markup.c - Content format normalization for BM25 indexing
 *
 * Strips HTML tags / Markdown syntax and produces plain visible
 * text suitable for feeding to to_tsvector_byid.
 */
#include <postgres.h>

#include <lib/stringinfo.h>
#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include <utils/builtins.h>
#include <varatt.h>

#include "types/markup.h"
#include "vendor/md4c/md4c.h"

/* ----------------------------------------------------------------
 * HTML normalization via libxml2
 * ----------------------------------------------------------------
 */

static bool
tp_html_skip_element(const xmlChar *name)
{
	return xmlStrcasecmp(name, (const xmlChar *)"script") == 0 ||
		   xmlStrcasecmp(name, (const xmlChar *)"style") == 0 ||
		   xmlStrcasecmp(name, (const xmlChar *)"template") == 0;
}

static void
tp_html_walk(xmlNode *node, StringInfo out)
{
	for (; node != NULL; node = node->next)
	{
		if (node->type == XML_TEXT_NODE && node->content != NULL)
		{
			if (out->len > 0)
				appendStringInfoChar(out, ' ');
			appendStringInfoString(out, (const char *)node->content);
		}
		else if (node->type == XML_ELEMENT_NODE)
		{
			if (tp_html_skip_element(node->name))
				continue;
			tp_html_walk(node->children, out);
		}
	}
}

static text *
tp_normalize_html(const char *input, int input_len)
{
	htmlDocPtr	   doc;
	xmlNode		  *root;
	StringInfoData out;

	doc = htmlReadMemory(
			input,
			input_len,
			NULL,
			"UTF-8",
			HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING |
					HTML_PARSE_NONET);
	if (doc == NULL)
		return cstring_to_text(input);

	initStringInfo(&out);

	root = xmlDocGetRootElement(doc);
	if (root != NULL)
		tp_html_walk(root, &out);

	xmlFreeDoc(doc);

	return cstring_to_text(out.data);
}

/*
 * State for HTML DOM walk with offset tracking.
 */
typedef struct TpHtmlMapState
{
	StringInfo	out;
	const char *orig;
	int			orig_len;
	int			orig_cursor;
	int		   *offset_map;
	int		   *end_map;
	int			map_alloc;
} TpHtmlMapState;

static void
tp_html_map_ensure(TpHtmlMapState *st)
{
	if (st->offset_map == NULL)
		return;
	if (st->out->len >= st->map_alloc)
	{
		st->map_alloc = Max(st->map_alloc * 2, 256);
		st->offset_map =
				repalloc(st->offset_map, sizeof(int) * st->map_alloc);
		st->end_map =
				repalloc(st->end_map, sizeof(int) * st->map_alloc);
	}
}

static void
tp_html_skip_tags(TpHtmlMapState *st)
{
	while (st->orig_cursor < st->orig_len &&
		   st->orig[st->orig_cursor] == '<')
	{
		while (st->orig_cursor < st->orig_len &&
			   st->orig[st->orig_cursor] != '>')
			st->orig_cursor++;
		if (st->orig_cursor < st->orig_len)
			st->orig_cursor++;
	}
}

static void
tp_html_skip_element_orig(TpHtmlMapState *st, const xmlChar *name)
{
	char	close_tag[64];
	int		close_len;

	close_len = snprintf(close_tag, sizeof(close_tag), "</%s>", name);
	if (close_len >= (int)sizeof(close_tag))
		close_len = sizeof(close_tag) - 1;

	while (st->orig_cursor < st->orig_len)
	{
		if (pg_strncasecmp(st->orig + st->orig_cursor,
						   close_tag, close_len) == 0)
		{
			st->orig_cursor += close_len;
			return;
		}
		st->orig_cursor++;
	}
}

static void
tp_html_walk_mapped(xmlNode *node, TpHtmlMapState *st)
{
	for (; node != NULL; node = node->next)
	{
		if (node->type == XML_TEXT_NODE && node->content != NULL)
		{
			const char *text = (const char *)node->content;
			int			text_len = strlen(text);

			if (st->out->len > 0)
			{
				tp_html_map_ensure(st);
				if (st->offset_map)
				{
					st->offset_map[st->out->len] = st->orig_cursor;
					st->end_map[st->out->len]	  = st->orig_cursor;
				}
				appendStringInfoChar(st->out, ' ');
			}

			tp_html_skip_tags(st);

			for (int i = 0; i < text_len; i++)
			{
				int char_start = st->orig_cursor;
				tp_html_map_ensure(st);
				if (st->orig_cursor < st->orig_len &&
					st->orig[st->orig_cursor] == '&')
				{
					while (st->orig_cursor < st->orig_len &&
						   st->orig[st->orig_cursor] != ';')
						st->orig_cursor++;
					if (st->orig_cursor < st->orig_len)
						st->orig_cursor++;
				}
				else
				{
					if (st->orig_cursor < st->orig_len)
						st->orig_cursor++;
				}
				if (st->offset_map)
				{
					st->offset_map[st->out->len] = char_start;
					st->end_map[st->out->len]	  = st->orig_cursor;
				}
				appendStringInfoChar(st->out, text[i]);
			}
		}
		else if (node->type == XML_ELEMENT_NODE)
		{
			if (tp_html_skip_element(node->name))
			{
				tp_html_skip_element_orig(st, node->name);
				continue;
			}
			tp_html_skip_tags(st);
			tp_html_walk_mapped(node->children, st);
		}
	}
}

/* ----------------------------------------------------------------
 * Markdown normalization via md4c
 * ----------------------------------------------------------------
 */

typedef struct TpMdState
{
	StringInfo	out;
	int			skip_depth;
	const char *input_base;
	int		   *offset_map;
	int		   *end_map;
	int			map_alloc;
} TpMdState;

static void
tp_md_map_byte(TpMdState *st, int orig_start, int orig_end)
{
	if (st->offset_map == NULL)
		return;
	if (st->out->len >= st->map_alloc)
	{
		st->map_alloc = Max(st->map_alloc * 2, 256);
		st->offset_map =
				repalloc(st->offset_map, sizeof(int) * st->map_alloc);
		st->end_map =
				repalloc(st->end_map, sizeof(int) * st->map_alloc);
	}
	st->offset_map[st->out->len] = orig_start;
	st->end_map[st->out->len]	  = orig_end;
}

static int
tp_md_enter_block(MD_BLOCKTYPE type, void *detail, void *userdata)
{
	TpMdState *st = (TpMdState *)userdata;

	(void)detail;

	if (type == MD_BLOCK_HTML)
	{
		st->skip_depth++;
		return 0;
	}

	if (st->out->len > 0 && st->skip_depth == 0)
	{
		tp_md_map_byte(st, -1, -1);
		appendStringInfoChar(st->out, ' ');
	}

	return 0;
}

static int
tp_md_leave_block(MD_BLOCKTYPE type, void *detail, void *userdata)
{
	TpMdState *st = (TpMdState *)userdata;

	(void)detail;

	if (type == MD_BLOCK_HTML)
		st->skip_depth--;

	return 0;
}

static int
tp_md_enter_span(MD_SPANTYPE type, void *detail, void *userdata)
{
	(void)type;
	(void)detail;
	(void)userdata;
	return 0;
}

static int
tp_md_leave_span(MD_SPANTYPE type, void *detail, void *userdata)
{
	(void)type;
	(void)detail;
	(void)userdata;
	return 0;
}

static int
tp_md_text(MD_TEXTTYPE type, const MD_CHAR *txt, MD_SIZE size, void *userdata)
{
	TpMdState *st = (TpMdState *)userdata;

	if (st->skip_depth > 0)
		return 0;

	switch (type)
	{
	case MD_TEXT_NORMAL:
	case MD_TEXT_CODE:
	case MD_TEXT_LATEXMATH:
	{
		int orig_off = (st->input_base) ? (int)(txt - st->input_base) : 0;
		if (st->out->len > 0 && size > 0)
		{
			tp_md_map_byte(st, orig_off, orig_off);
			appendStringInfoChar(st->out, ' ');
		}
		for (MD_SIZE i = 0; i < size; i++)
		{
			tp_md_map_byte(st, orig_off + (int)i,
						   orig_off + (int)i + 1);
			appendStringInfoChar(st->out, txt[i]);
		}
		break;
	}

	case MD_TEXT_ENTITY:
	{
		int orig_off = (st->input_base) ? (int)(txt - st->input_base) : 0;
		int ent_end	 = orig_off + (int)size;
		if (size == 5 && memcmp(txt, "&amp;", 5) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, '&');
		}
		else if (size == 4 && memcmp(txt, "&lt;", 4) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, '<');
		}
		else if (size == 4 && memcmp(txt, "&gt;", 4) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, '>');
		}
		else if (size == 6 && memcmp(txt, "&quot;", 6) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, '"');
		}
		else if (size == 6 && memcmp(txt, "&apos;", 6) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, '\'');
		}
		else if (size == 6 && memcmp(txt, "&nbsp;", 6) == 0)
		{
			tp_md_map_byte(st, orig_off, ent_end);
			appendStringInfoChar(st->out, ' ');
		}
		break;
	}

	case MD_TEXT_SOFTBR:
	case MD_TEXT_BR:
	{
		int orig_off = (st->input_base) ? (int)(txt - st->input_base) : 0;
		tp_md_map_byte(st, orig_off, orig_off + (int)size);
		appendStringInfoChar(st->out, ' ');
		break;
	}

	case MD_TEXT_NULLCHAR:
		break;

	case MD_TEXT_HTML:
		break;
	}

	return 0;
}

static text *
tp_normalize_markdown_internal(const char *input, int input_len,
							   TpOffsetMap **map_out)
{
	StringInfoData out;
	TpMdState	   state;
	MD_PARSER	   parser;
	int			   ret;

	initStringInfo(&out);
	state.out		  = &out;
	state.skip_depth  = 0;
	state.input_base  = input;
	state.offset_map  = NULL;
	state.map_alloc	  = 0;

	if (map_out)
	{
		state.map_alloc	 = Max(input_len, 64);
		state.offset_map = palloc(sizeof(int) * state.map_alloc);
		state.end_map	 = palloc(sizeof(int) * state.map_alloc);
	}

	memset(&parser, 0, sizeof(parser));
	parser.abi_version = 0;
	parser.flags	   = MD_DIALECT_GITHUB | MD_FLAG_NOHTML;
	parser.enter_block = tp_md_enter_block;
	parser.leave_block = tp_md_leave_block;
	parser.enter_span  = tp_md_enter_span;
	parser.leave_span  = tp_md_leave_span;
	parser.text		   = tp_md_text;

	ret = md_parse(input, (MD_SIZE)input_len, &parser, &state);
	if (ret != 0)
	{
		if (state.offset_map)
			pfree(state.offset_map);
		if (state.end_map)
			pfree(state.end_map);
		if (map_out)
			*map_out = NULL;
		return cstring_to_text(input);
	}

	if (map_out && state.offset_map)
	{
		TpOffsetMap *map	  = palloc(sizeof(TpOffsetMap));
		int			 norm_len = out.len;

		if (norm_len >= state.map_alloc)
		{
			state.offset_map = repalloc(state.offset_map,
										sizeof(int) * (norm_len + 1));
			state.end_map = repalloc(state.end_map,
									 sizeof(int) * (norm_len + 1));
		}
		state.offset_map[norm_len] = input_len;
		state.end_map[norm_len]	   = input_len;

		/* Resolve sentinel values for separator spaces */
		for (int i = 0; i < norm_len; i++)
		{
			if (state.offset_map[i] < 0)
			{
				state.offset_map[i] = (i + 1 < norm_len)
										? state.offset_map[i + 1]
										: input_len;
				state.end_map[i] = state.offset_map[i];
			}
		}

		map->offsets	 = state.offset_map;
		map->end_offsets = state.end_map;
		map->len		 = norm_len;
		*map_out		 = map;
	}
	else if (map_out)
	{
		*map_out = NULL;
	}

	return cstring_to_text(out.data);
}

static text *
tp_normalize_markdown(const char *input, int input_len)
{
	return tp_normalize_markdown_internal(input, input_len, NULL);
}

/* ----------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------
 */

text *
tp_normalize_markup(text *input, TpContentFormat fmt)
{
	const char *raw;
	int			len;

	if (fmt == TP_FORMAT_PLAIN)
		return input;

	raw = VARDATA_ANY(input);
	len = VARSIZE_ANY_EXHDR(input);

	switch (fmt)
	{
	case TP_FORMAT_PLAIN:
		return input;
	case TP_FORMAT_HTML:
		return tp_normalize_html(raw, len);
	case TP_FORMAT_MARKDOWN:
		return tp_normalize_markdown(raw, len);
	}

	return input;
}

text *
tp_normalize_markup_with_map(text *input, TpContentFormat fmt,
							 TpOffsetMap **map_out)
{
	const char *raw;
	int			len;
	text	   *result;

	if (map_out)
		*map_out = NULL;

	if (fmt == TP_FORMAT_PLAIN)
		return input;

	raw = VARDATA_ANY(input);
	len = VARSIZE_ANY_EXHDR(input);

	switch (fmt)
	{
	case TP_FORMAT_HTML:
		if (map_out)
		{
			htmlDocPtr	   doc;
			xmlNode		  *root;
			TpHtmlMapState st;
			StringInfoData buf;

			doc = htmlReadMemory(
					raw, len, NULL, "UTF-8",
					HTML_PARSE_RECOVER | HTML_PARSE_NOERROR |
							HTML_PARSE_NOWARNING | HTML_PARSE_NONET);
			if (doc == NULL)
				return input;

			initStringInfo(&buf);
			st.out		   = &buf;
			st.orig		   = raw;
			st.orig_len	   = len;
			st.orig_cursor = 0;
			st.map_alloc   = Max(len, 64);
			st.offset_map  = palloc(sizeof(int) * st.map_alloc);
			st.end_map	   = palloc(sizeof(int) * st.map_alloc);

			root = xmlDocGetRootElement(doc);
			if (root != NULL)
				tp_html_walk_mapped(root, &st);
			xmlFreeDoc(doc);

			{
				TpOffsetMap *map	  = palloc(sizeof(TpOffsetMap));
				int			 norm_len = buf.len;
				if (norm_len >= st.map_alloc)
				{
					st.offset_map = repalloc(st.offset_map,
											 sizeof(int) * (norm_len + 1));
					st.end_map = repalloc(st.end_map,
										  sizeof(int) * (norm_len + 1));
				}
				st.offset_map[norm_len] = st.orig_cursor;
				st.end_map[norm_len]	= st.orig_cursor;
				map->offsets			= st.offset_map;
				map->end_offsets		= st.end_map;
				map->len				= norm_len;
				*map_out				= map;
			}

			return cstring_to_text(buf.data);
		}
		return tp_normalize_html(raw, len);

	case TP_FORMAT_MARKDOWN:
		return tp_normalize_markdown_internal(raw, len, map_out);

	case TP_FORMAT_PLAIN:
		break;
	}

	return input;
}

TpContentFormat
tp_parse_content_format(const char *str)
{
	if (str == NULL || str[0] == '\0' || pg_strcasecmp(str, "plain") == 0)
		return TP_FORMAT_PLAIN;
	if (pg_strcasecmp(str, "html") == 0)
		return TP_FORMAT_HTML;
	if (pg_strcasecmp(str, "markdown") == 0 || pg_strcasecmp(str, "md") == 0)
		return TP_FORMAT_MARKDOWN;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid content_format: \"%s\"", str),
			 errhint("Valid values are \"plain\", \"html\", "
					 "and \"markdown\".")));
	return TP_FORMAT_PLAIN; /* unreachable */
}

const char *
tp_content_format_name(TpContentFormat fmt)
{
	switch (fmt)
	{
	case TP_FORMAT_PLAIN:
		return "plain";
	case TP_FORMAT_HTML:
		return "html";
	case TP_FORMAT_MARKDOWN:
		return "markdown";
	}
	return "plain";
}
