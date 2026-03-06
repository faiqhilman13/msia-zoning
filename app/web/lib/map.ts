import type { Filters, LayerType } from "@/lib/types";
import { buildTileFilter } from "@/lib/filters";

const TILE_BASE = process.env.NEXT_PUBLIC_PGTILESERV_URL ?? "http://localhost:7800";

export const layerColors: Record<LayerType, [number, number, number]> = {
  kebenaran_merancang: [41, 109, 84],
  pelan_bangunan: [34, 87, 155],
  kerja_tanah: [194, 110, 38]
};

export function buildTilesUrl(filters: Filters) {
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

export function boundariesUrl() {
  return `${TILE_BASE}/marts.mbjb_context_boundaries/{z}/{x}/{y}.pbf`;
}
