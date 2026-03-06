import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    municipality: "MBJB",
    sourceSystem: "MBJB GeoJB",
    sourceUrl:
      "https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson",
    observedCounts: {
      pelan_bangunan: 618,
      kebenaran_merancang: 310,
      kerja_tanah: 190
    },
    notes: [
      "Public MBJB GeoJB ArcGIS REST data.",
      "Source geometry is polygon geometry.",
      "Raw snapshots are kept privately on disk and not exposed directly in the browser.",
      "Owner fields such as PEMILIK are not exposed in the public UI."
    ]
  });
}
