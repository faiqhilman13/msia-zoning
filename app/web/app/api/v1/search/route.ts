import { NextRequest, NextResponse } from "next/server";

import { pool } from "@/lib/db";
import { parseMunicipalities } from "@/lib/municipalities";

export async function GET(request: NextRequest) {
  const q = request.nextUrl.searchParams.get("q")?.trim();
  if (!q) {
    return NextResponse.json([]);
  }
  const municipalities = parseMunicipalities(request.nextUrl.searchParams);

  const client = await pool.connect();
  try {
    const result = await client.query(
      `
      SELECT
        f.application_id,
        f.reference_no,
        f.public_display_title,
        f.layer_type,
        f.public_display_status,
        f.planning_block,
        f.mukim,
        f.source_municipality,
        f.has_geometry,
        f.centroid_lon,
        f.centroid_lat,
        ts_rank_cd(s.search_tsv, plainto_tsquery('simple', $1)) AS rank
      FROM marts.public_applications f
      JOIN core.search_documents s ON s.application_id = f.application_id
      WHERE f.source_municipality = ANY($3)
        AND (
          s.search_tsv @@ plainto_tsquery('simple', $1)
          OR f.reference_no ILIKE $2
          OR f.public_display_title ILIKE $2
          OR COALESCE(f.planning_block, '') ILIKE $2
          OR COALESCE(f.mukim, '') ILIKE $2
        )
      ORDER BY f.has_geometry DESC, rank DESC, f.reference_no NULLS LAST
      LIMIT 12
      `,
      [q, `%${q}%`, municipalities]
    );

    return NextResponse.json(
      result.rows.map((row) => ({
        applicationId: row.application_id,
        referenceNo: row.reference_no,
        title: row.public_display_title,
        layerType: row.layer_type,
        status: row.public_display_status,
        municipality: row.source_municipality,
        hasGeometry: row.has_geometry,
        planningBlock: row.planning_block,
        mukim: row.mukim,
        centroidLon: row.centroid_lon == null ? null : Number(row.centroid_lon),
        centroidLat: row.centroid_lat == null ? null : Number(row.centroid_lat)
      }))
    );
  } finally {
    client.release();
  }
}
