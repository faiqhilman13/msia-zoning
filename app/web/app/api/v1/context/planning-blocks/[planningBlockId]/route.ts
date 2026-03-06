import { NextRequest, NextResponse } from "next/server";

import { pool } from "@/lib/db";

type Params = {
  params: Promise<{ planningBlockId: string }>;
};

export const dynamic = "force-dynamic";

export async function GET(_request: NextRequest, { params }: Params) {
  const { planningBlockId } = await params;
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
        round(ST_Area(ST_Transform(p.geometry, 3857))::numeric, 2) AS area_m2,
        round((ST_Area(ST_Transform(p.geometry, 3857)) / 4046.8564224)::numeric, 4) AS area_acres,
        COALESCE(stats.feature_count, 0) AS feature_count,
        COALESCE(stats.approved_count, 0) AS approved_count,
        COALESCE(stats.pending_count, 0) AS pending_count,
        COALESCE(stats.total_development_area_acres, 0) AS total_development_area_acres,
        COALESCE(layer_rows.layer_breakdown, '[]'::json) AS layer_breakdown
      FROM marts.mbjb_context_planning_blocks p
      LEFT JOIN LATERAL (
        SELECT
          count(*)::int AS feature_count,
          count(*) FILTER (WHERE f.public_display_status = 'approved')::int AS approved_count,
          count(*) FILTER (WHERE f.public_display_status = 'pending')::int AS pending_count,
          COALESCE(round(sum(f.area_acres)::numeric, 2), 0) AS total_development_area_acres
        FROM marts.mbjb_public_features f
        WHERE f.planning_block = p.planning_block
      ) stats ON true
      LEFT JOIN LATERAL (
        SELECT
          json_agg(
            json_build_object(
              'layerType', layer_counts.layer_type,
              'count', layer_counts.feature_count
            )
            ORDER BY layer_counts.layer_type
          ) AS layer_breakdown
        FROM (
          SELECT
            f.layer_type,
            count(*)::int AS feature_count
          FROM marts.mbjb_public_features f
          WHERE f.planning_block = p.planning_block
          GROUP BY f.layer_type
        ) layer_counts
      ) layer_rows ON true
      WHERE p.zoning_area_id = $1
      `,
      [planningBlockId]
    );

    if (!result.rowCount) {
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    const row = result.rows[0];
    return NextResponse.json({
      kind: "planning_block",
      planningBlockId: row.zoning_area_id,
      planningBlock: row.planning_block,
      zoningName: row.zoning_name,
      mukim: row.mukim,
      areaAcres: Number(row.area_acres),
      areaM2: Number(row.area_m2),
      centroidLon: Number(row.centroid_lon),
      centroidLat: Number(row.centroid_lat),
      featureCount: Number(row.feature_count),
      approvedCount: Number(row.approved_count),
      pendingCount: Number(row.pending_count),
      totalDevelopmentAreaAcres: Number(row.total_development_area_acres),
      layerBreakdown: row.layer_breakdown ?? []
    });
  } finally {
    client.release();
  }
}
