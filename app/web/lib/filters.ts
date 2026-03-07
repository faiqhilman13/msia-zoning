import type { Filters, LayerType, MunicipalityCode } from "@/lib/types";

const LAYER_OPTIONS_BY_MUNICIPALITY: Record<
  MunicipalityCode,
  Array<{ key: LayerType; label: string }>
> = {
  MBJB: [
    { key: "kebenaran_merancang", label: "Kebenaran Merancang" },
    { key: "pelan_bangunan", label: "Pelan Bangunan" },
    { key: "kerja_tanah", label: "Kerja Tanah" }
  ],
  MBPJ: [{ key: "approved_project_register", label: "Approved Register" }]
};

const ALL_LAYERS = Object.values(LAYER_OPTIONS_BY_MUNICIPALITY).flatMap((items) =>
  items.map((item) => item.key)
);

function parseList(searchParams: URLSearchParams, key: string): string[] {
  return searchParams
    .getAll(key)
    .flatMap((item) => item.split(","))
    .map((item) => item.trim())
    .filter(Boolean);
}

export function parseFilters(searchParams: URLSearchParams): Filters {
  const layerTypes = parseList(searchParams, "layer").filter((value): value is LayerType =>
    ALL_LAYERS.includes(value as LayerType)
  );
  const years = parseList(searchParams, "year")
    .map((value) => Number.parseInt(value, 10))
    .filter((value) => Number.isFinite(value));
  return {
    layerTypes: layerTypes.length ? layerTypes : ALL_LAYERS,
    statuses: parseList(searchParams, "status"),
    years,
    planningBlocks: parseList(searchParams, "planningBlock"),
    mukims: parseList(searchParams, "mukim")
  };
}

export function buildSqlFilter(filters: Filters) {
  const clauses: string[] = [];
  const values: Array<string | number | string[] | number[]> = [];

  clauses.push(`layer_type = ANY($${values.length + 1})`);
  values.push(filters.layerTypes);

  if (filters.statuses.length) {
    clauses.push(`public_display_status = ANY($${values.length + 1})`);
    values.push(filters.statuses);
  }
  if (filters.years.length) {
    clauses.push(`COALESCE(approval_year, application_year) = ANY($${values.length + 1})`);
    values.push(filters.years);
  }
  if (filters.planningBlocks.length) {
    clauses.push(`planning_block = ANY($${values.length + 1})`);
    values.push(filters.planningBlocks);
  }
  if (filters.mukims.length) {
    clauses.push(`mukim = ANY($${values.length + 1})`);
    values.push(filters.mukims);
  }

  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    values
  };
}

function quoteCql(value: string) {
  return `'${value.replaceAll("'", "''")}'`;
}

export function buildTileFilter(filters: Filters) {
  const clauses: string[] = [];
  if (filters.layerTypes.length) {
    clauses.push(`layer_type IN (${filters.layerTypes.map(quoteCql).join(",")})`);
  }
  if (filters.statuses.length) {
    clauses.push(`public_display_status IN (${filters.statuses.map(quoteCql).join(",")})`);
  }
  if (filters.planningBlocks.length) {
    clauses.push(`planning_block IN (${filters.planningBlocks.map(quoteCql).join(",")})`);
  }
  if (filters.mukims.length) {
    clauses.push(`mukim IN (${filters.mukims.map(quoteCql).join(",")})`);
  }
  if (filters.years.length) {
    clauses.push(
      `(${filters.years
        .map((year) => `(application_year = ${year} OR approval_year = ${year})`)
        .join(" OR ")})`
    );
  }
  return clauses.join(" AND ");
}

export function hasActiveFilters(filters: Filters) {
  return (
    filters.layerTypes.length !== ALL_LAYERS.length ||
    filters.statuses.length > 0 ||
    filters.years.length > 0 ||
    filters.planningBlocks.length > 0 ||
    filters.mukims.length > 0
  );
}

export function getLayerOptions(municipality: MunicipalityCode) {
  return LAYER_OPTIONS_BY_MUNICIPALITY[municipality];
}

export function getDefaultLayerTypes(municipality: MunicipalityCode) {
  return getLayerOptions(municipality).map((item) => item.key);
}

export function getDefaultFilters(municipality: MunicipalityCode): Filters {
  return {
    layerTypes: getDefaultLayerTypes(municipality),
    statuses: [],
    years: [],
    planningBlocks: [],
    mukims: []
  };
}

export const DEFAULT_FILTERS: Filters = getDefaultFilters("MBJB");
