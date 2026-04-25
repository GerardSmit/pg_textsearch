/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * scan.h - Memtable scan operations
 */
#pragma once

#include <postgres.h>

#include <access/relscan.h>

#include "access/am.h"
#include "index/metapage.h"
#include "index/state.h"
#include "types/vector.h"

/*
 * Search the memtable and segments for documents matching the query vector.
 * Results are stored in the scan opaque structure.
 *
 * Returns true on success (at least one result), false otherwise.
 */
bool tp_memtable_search(
		IndexScanDesc	   scan,
		TpLocalIndexState *index_state,
		TpVector		  *query_vector,
		TpIndexMetaPage	   metap);

/*
 * Search using raw query text with grammar extensions (prefix '*').
 * Used when the query contains tokens the existing tsvector-based
 * path can't represent verbatim. Parses the text, tokenizes regular
 * terms through the index's text_config, dictionary-expands prefix
 * terms, and feeds the unified term list to BMW.
 *
 * Returns true on success (at least one result), false otherwise.
 */
bool tp_memtable_search_with_grammar(
		IndexScanDesc	   scan,
		TpLocalIndexState *index_state,
		const char		  *query_text,
		TpIndexMetaPage	   metap,
		bool			   fuzzy,
		uint8			   fuzzy_max_distance);
