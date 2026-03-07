import { NextRequest, NextResponse } from "next/server";

import { parseMunicipalities } from "@/lib/municipalities";
import { getFilterOptions } from "@/lib/queries";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const options = await getFilterOptions({
    municipalities: parseMunicipalities(request.nextUrl.searchParams)
  });
  return NextResponse.json(options);
}
