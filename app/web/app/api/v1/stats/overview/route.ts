import { NextRequest, NextResponse } from "next/server";

import { parseFilters } from "@/lib/filters";
import { parseMunicipalities } from "@/lib/municipalities";
import { getOverview } from "@/lib/queries";

export async function GET(request: NextRequest) {
  const filters = parseFilters(request.nextUrl.searchParams);
  const overview = await getOverview(filters, {
    municipalities: parseMunicipalities(request.nextUrl.searchParams)
  });
  return NextResponse.json({
    totalFeatures: Number(overview.total_features),
    approvedFeatures: Number(overview.approved_features),
    pendingFeatures: Number(overview.pending_features),
    totalAreaAcres: Number(overview.total_area_acres)
  });
}
