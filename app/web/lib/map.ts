import type { Filters, LayerType, MunicipalityCode } from "@/lib/types";
import { buildTileFilter } from "@/lib/filters";

const TILE_BASE = process.env.NEXT_PUBLIC_PGTILESERV_URL ?? "http://localhost:7800";

function readNumericEnv(name: string, fallback: number) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? value : fallback;
}

export const layerColors: Record<LayerType, [number, number, number]> = {
  kebenaran_merancang: [41, 109, 84],
  pelan_bangunan: [34, 87, 155],
  kerja_tanah: [194, 110, 38],
  approved_project_register: [88, 98, 94]
};

export function getDefaultViewport(municipality: MunicipalityCode) {
  if (municipality === "MBPJ") {
    return {
      lon: readNumericEnv("NEXT_PUBLIC_MBPJ_DEFAULT_LON", 101.6237),
      lat: readNumericEnv("NEXT_PUBLIC_MBPJ_DEFAULT_LAT", 3.1073),
      zoom: readNumericEnv("NEXT_PUBLIC_MBPJ_DEFAULT_ZOOM", 12)
    };
  }

  return {
    lon: readNumericEnv("NEXT_PUBLIC_DEFAULT_LON", 103.7414),
    lat: readNumericEnv("NEXT_PUBLIC_DEFAULT_LAT", 1.4927),
    zoom: readNumericEnv("NEXT_PUBLIC_DEFAULT_ZOOM", 11)
  };
}

export function buildTilesUrl(filters: Filters, municipality: MunicipalityCode) {
  if (municipality !== "MBJB") {
    return null;
  }
  const template = `${TILE_BASE}/marts.mbjb_public_features/{z}/{x}/{y}.pbf`;
  const filter = buildTileFilter(filters);
  if (!filter) {
    return template;
  }
  const params = new URLSearchParams({ filter });
  return `${template}?${params.toString()}`;
}

export function planningBlocksUrl() {
  return `${TILE_BASE}/marts.mbjb_context_planning_blocks/{z}/{x}/{y}.pbf`;
}

export function contextBuildingsUrl() {
  return `${TILE_BASE}/marts.mbpj_context_buildings/{z}/{x}/{y}.pbf`;
}

export function boundariesUrl(municipality: MunicipalityCode) {
  return municipality === "MBPJ"
    ? `${TILE_BASE}/marts.mbpj_context_boundaries/{z}/{x}/{y}.pbf`
    : `${TILE_BASE}/marts.mbjb_context_boundaries/{z}/{x}/{y}.pbf`;
}
