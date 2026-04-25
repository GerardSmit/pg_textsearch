/*
 * Copyright (c) 2025-2026 Tiger Data, Inc.
 * Licensed under the PostgreSQL License. See LICENSE for details.
 *
 * cost.c - Cost estimation for BM25 index scans
 */
#include <postgres.h>

#include <access/genam.h>
#include <utils/float.h>
#include <utils/rel.h>
#include <utils/selfuncs.h>

#include "constants.h"
#include "index/limit.h"
#include "index/metapage.h"
#include "planner/cost.h"

/*
 * Estimate cost of BM25 index scan
 */
void
tp_costestimate(
		PlannerInfo *root,
		IndexPath	*path,
		double		 loop_count,
		Cost		*indexStartupCost,
		Cost		*indexTotalCost,
		Selectivity *indexSelectivity,
		double		*indexCorrelation,
		double		*indexPages)
{
	GenericCosts	costs;
	TpIndexMetaPage metap;
	double			num_tuples	 = TP_DEFAULT_TUPLE_ESTIMATE;
	int				num_segments = 0;

	/* Never use index without ORDER BY clause */
	if (!path->indexorderbys || list_length(path->indexorderbys) == 0)
	{
		*indexStartupCost = get_float8_infinity();
		*indexTotalCost	  = get_float8_infinity();
		return;
	}

	/* Check for LIMIT clause and verify it can be safely pushed down */
	if (root && root->limit_tuples > 0 && root->limit_tuples < INT_MAX)
	{
		int limit = (int)root->limit_tuples;

		if (tp_can_pushdown_limit(root, path, limit))
			tp_store_query_limit(path->indexinfo->indexoid, limit);
	}

	/* Try to get actual statistics from the index */
	if (path->indexinfo && path->indexinfo->indexoid != InvalidOid)
	{
		Relation index_rel =
				index_open(path->indexinfo->indexoid, AccessShareLock);

		if (index_rel)
		{
			metap = tp_get_metapage(index_rel);
			if (metap && metap->total_docs > 0)
				num_tuples = (double)metap->total_docs;

			/*
			 * Count segments — parallel scan distributes segment
			 * scoring across workers, so more segments = bigger win.
			 */
			if (metap)
			{
				int level;

				for (level = 0; level < TP_MAX_LEVELS; level++)
				{
					BlockNumber head = metap->level_heads[level];

					while (head != InvalidBlockNumber)
					{
						num_segments++;
						break; /* counting chains is expensive;
								* just detect non-empty level */
					}
				}
				pfree(metap);
			}

			index_close(index_rel, AccessShareLock);
		}
	}

	/* Initialize generic costs */
	MemSet (&costs, 0, sizeof(costs))
		;
	genericcostestimate(root, path, loop_count, &costs);

	/* Override with BM25-specific estimates */
	*indexStartupCost = costs.indexStartupCost + 0.01;
	*indexTotalCost	  = costs.indexTotalCost * TP_INDEX_SCAN_COST_FACTOR;

	/*
	 * Parallel cost nudge.  Benchmarking shows parallel scan only
	 * wins when total_docs >= ~1M (BMW skip lists make smaller
	 * indexes too fast for Gather overhead to amortize).  At 2.5M+
	 * rows the speedup is 2-3x.  Only apply when segments exist
	 * for workers to claim.
	 */
	if (num_segments >= 2 && num_tuples >= 1000000)
		*indexTotalCost *= 0.5;

	/*
	 * Calculate selectivity based on LIMIT if available, otherwise default
	 */
	if (root && root->limit_tuples > 0 && root->limit_tuples < INT_MAX &&
		num_tuples > 0)
	{
		/* Use LIMIT as upper bound for selectivity calculation */
		double limit_selectivity = Min(1.0, root->limit_tuples / num_tuples);
		*indexSelectivity =
				Max(limit_selectivity, TP_DEFAULT_INDEX_SELECTIVITY);
	}
	else
	{
		*indexSelectivity = TP_DEFAULT_INDEX_SELECTIVITY;
	}
	*indexCorrelation = 0.0; /* No correlation assumptions */
	*indexPages		  = Max(1.0, num_tuples / 100.0); /* Rough page estimate */
}
