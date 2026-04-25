/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * highlight.h - SQL-callable BM25 highlighting functions
 */
#pragma once

#include <postgres.h>

#include <fmgr.h>

Datum bm25_snippet(PG_FUNCTION_ARGS);
Datum bm25_snippet_text(PG_FUNCTION_ARGS);
Datum bm25_snippet_positions(PG_FUNCTION_ARGS);
Datum bm25_snippet_positions_text(PG_FUNCTION_ARGS);
Datum bm25_highlights(PG_FUNCTION_ARGS);
