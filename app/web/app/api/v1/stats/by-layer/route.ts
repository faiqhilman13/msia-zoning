import { NextRequest, NextResponse } from "next/server";

import { parseFilters } from "@/lib/filters";
import { getDistribution } from "@/lib/queries";

export async function GET(request: NextRequest) {
  const filters = parseFilters(request.nextUrl.searchParams);
  const rows = await getDistribution(filters, "layer_type");
  return NextResponse.json(rows);
}
