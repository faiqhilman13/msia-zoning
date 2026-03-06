import { NextRequest, NextResponse } from "next/server";

import { pool } from "@/lib/db";

type Params = {
  params: Promise<{ applicationId: string }>;
};

export async function GET(_request: NextRequest, { params }: Params) {
  const { applicationId } = await params;
  const client = await pool.connect();
  try {
    const result = await client.query(
      `
      SELECT
        f.application_id,
        f.reference_no,
        f.reference_no_alt,
        f.public_display_title,
        f.public_display_status,
        f.layer_type,
        f.application_type,
        f.application_year,
        f.approval_year,
        f.lot_no,
        f.mukim,
        f.planning_block,
        f.zoning_name,
        f.developer_name,
        f.consultant_name,
        f.area_acres,
        f.area_m2,
        f.centroid_lon,
        f.centroid_lat
      FROM marts.mbjb_public_features f
      WHERE f.application_id = $1
      `,
      [applicationId]
    );

    if (!result.rowCount) {
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    const row = result.rows[0];
    return NextResponse.json({
      kind: "application",
      applicationId: row.application_id,
      referenceNo: row.reference_no,
      referenceNoAlt: row.reference_no_alt,
      title: row.public_display_title,
      status: row.public_display_status,
      layerType: row.layer_type,
      applicationType: row.application_type,
      applicationYear: row.application_year,
      approvalYear: row.approval_year,
      lotNo: row.lot_no,
      mukim: row.mukim,
      planningBlock: row.planning_block,
      zoningName: row.zoning_name,
      developerName: row.developer_name,
      consultantName: row.consultant_name,
      areaAcres: Number(row.area_acres),
      areaM2: Number(row.area_m2),
      centroidLon: Number(row.centroid_lon),
      centroidLat: Number(row.centroid_lat)
    });
  } finally {
    client.release();
  }
}
