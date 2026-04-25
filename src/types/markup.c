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
		   xmlStrcasecmp(name, (const xmlChar *)"style") == 0;
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

/* ----------------------------------------------------------------
 * Markdown normalization via md4c
 * ----------------------------------------------------------------
 */

typedef struct TpMdState
{
	StringInfo out;
	int		   skip_depth;
} TpMdState;

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
		appendStringInfoChar(st->out, ' ');

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
		if (st->out->len > 0 && size > 0)
			appendStringInfoChar(st->out, ' ');
		appendBinaryStringInfo(st->out, txt, (int)size);
		break;

	case MD_TEXT_ENTITY:
		/*
		 * md4c passes entity verbatim (e.g. "&amp;").
		 * Decode common ones; skip unknown.
		 */
		if (size == 5 && memcmp(txt, "&amp;", 5) == 0)
			appendStringInfoChar(st->out, '&');
		else if (size == 4 && memcmp(txt, "&lt;", 4) == 0)
			appendStringInfoChar(st->out, '<');
		else if (size == 4 && memcmp(txt, "&gt;", 4) == 0)
			appendStringInfoChar(st->out, '>');
		else if (size == 6 && memcmp(txt, "&quot;", 6) == 0)
			appendStringInfoChar(st->out, '"');
		else if (size == 6 && memcmp(txt, "&apos;", 6) == 0)
			appendStringInfoChar(st->out, '\'');
		else if (size == 6 && memcmp(txt, "&nbsp;", 6) == 0)
			appendStringInfoChar(st->out, ' ');
		break;

	case MD_TEXT_SOFTBR:
	case MD_TEXT_BR:
		appendStringInfoChar(st->out, ' ');
		break;

	case MD_TEXT_NULLCHAR:
		break;

	case MD_TEXT_HTML:
		/* Inline raw HTML — skip */
		break;
	}

	return 0;
}

static text *
tp_normalize_markdown(const char *input, int input_len)
{
	StringInfoData out;
	TpMdState	   state;
	MD_PARSER	   parser;
	int			   ret;

	initStringInfo(&out);
	state.out		 = &out;
	state.skip_depth = 0;

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
		return cstring_to_text(input);

	return cstring_to_text(out.data);
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
