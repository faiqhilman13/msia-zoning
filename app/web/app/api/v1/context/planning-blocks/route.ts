import { NextResponse } from "next/server";

import { pool } from "@/lib/db";
import { buildSqlFilter, hasActiveFilters, parseFilters } from "@/lib/filters";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const filters = parseFilters(new URL(request.url).searchParams);
  const { whereSql, values } = buildSqlFilter(filters);
  const activeFilters = hasActiveFilters(filters);
  const statsWhereSql = whereSql
    ? `${whereSql} AND f.planning_block = p.planning_block`
    : `WHERE f.planning_block = p.planning_block`;

  const client = await pool.connect();
  try {
    const result = await client.query(
      `
      SELECT
        p.zoning_area_id,
        p.planning_block,
        p.zoning_name,
        p.mukim,
        ST_X(ST_PointOnSurface(p.geometry)) AS centroid_lon,
        ST_Y(ST_PointOnSurface(p.geometry)) AS centroid_lat,
        COALESCE(stats.feature_count, 0) AS feature_count
      FROM marts.mbjb_context_planning_blocks p
      LEFT JOIN LATERAL (
        SELECT count(*)::int AS feature_count
        FROM marts.mbjb_public_features f
        ${statsWhereSql}
      ) stats ON true
      WHERE p.planning_block IS NOT NULL
      ${activeFilters ? "AND COALESCE(stats.feature_count, 0) > 0" : ""}
      ORDER BY p.planning_block
      `,
      values
    );

    return NextResponse.json(
      result.rows.map((row) => ({
        planningBlockId: row.zoning_area_id,
        planningBlock: row.planning_block,
        zoningName: row.zoning_name,
        mukim: row.mukim,
        centroidLon: Number(row.centroid_lon),
        centroidLat: Number(row.centroid_lat),
        featureCount: Number(row.feature_count)
      }))
    );
  } finally {
    client.release();
  }
}
