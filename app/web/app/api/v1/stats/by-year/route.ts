import { NextRequest, NextResponse } from "next/server";

import { parseFilters } from "@/lib/filters";
import { parseMunicipalities } from "@/lib/municipalities";
import { getDistribution } from "@/lib/queries";

export async function GET(request: NextRequest) {
  const filters = parseFilters(request.nextUrl.searchParams);
  const rows = await getDistribution(filters, "year", {
    municipalities: parseMunicipalities(request.nextUrl.searchParams)
  });
  return NextResponse.json(rows);
}
