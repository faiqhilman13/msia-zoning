import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    sources: [
      {
        municipality: "MBJB",
        mode: "map_enabled",
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
      },
      {
        municipality: "MBPJ",
        mode: "context_geometry_only",
        sourceSystem: "MBPJ SmartDev + MBPJ ArcGIS",
        sourceUrl: "https://mbpjsmartdev.pjsmartcity.gov.my/",
        observedCounts: {
          approved_project_register: 62,
          official_buildings: 57,
          municipality_boundary: 1
        },
        notes: [
          "The public MBPJ SmartDev homepage exposes an HTML approved-project register.",
          "Raw homepage, feedback lookup, terms, disclaimer, and GIS landing snapshots are preserved on disk per run.",
          "A public MBPJ ArcGIS service exposes official-building polygons and the municipal boundary for context rendering.",
          "These geometries are not direct SmartDev project polygons, so MBPJ project rows remain searchable text records with null project geometry.",
          "Owner-like party text extracted from titles is stored internally as owner_name_raw and not exposed by default."
        ]
      }
    ]
  });
}
