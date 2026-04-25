/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * posting_entry.h - Common posting entry type
 *
 * This type is shared between the DSA-based shared memtable and the
 * palloc-based local memtable used in parallel builds.
 */
#pragma once

#include <postgres.h>

#include <storage/itemptr.h>
#include <utils/dsa.h>

/*
 * Individual document occurrence within a posting list.
 * Doc IDs are assigned at segment write time via docmap lookup.
 *
 * positions_dp points to a uint32[frequency] array in DSA holding
 * the 1-based ordinal position of each term occurrence within the
 * source doc (in tsvector order). Set to InvalidDsaPointer when:
 *   - the index was built with `with_positions = false`, or
 *   - this entry came from a V5-only build path (no positions
 *     captured at the time).
 *
 * Struct grew from 8 → 24 bytes in v1.4 (positions support). This
 * is a SHARED-MEMORY FORMAT BREAK — operators MUST restart the
 * server after upgrading the binary.
 */
typedef struct TpPostingEntry
{
	ItemPointerData ctid;		  /* 6: heap tuple ID */
	uint16			_pad;		  /* 2: alignment */
	int32			frequency;	  /* 4: term frequency (== position count) */
	dsa_pointer		positions_dp; /* 8: uint32[frequency] or Invalid */
} TpPostingEntry;
