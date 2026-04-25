/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * markup.h - Content format normalization for BM25 indexing
 */
#ifndef TP_MARKUP_H
#define TP_MARKUP_H

#include <postgres.h>

typedef enum TpContentFormat
{
	TP_FORMAT_PLAIN	   = 0,
	TP_FORMAT_HTML	   = 1,
	TP_FORMAT_MARKDOWN = 2
} TpContentFormat;

#define TP_FORMAT_COUNT 3

extern text *tp_normalize_markup(text *input, TpContentFormat fmt);

extern TpContentFormat tp_parse_content_format(const char *str);
extern const char	  *tp_content_format_name(TpContentFormat fmt);

#endif /* TP_MARKUP_H */
