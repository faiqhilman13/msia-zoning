import { pool } from "@/lib/db";
import { buildSqlFilter } from "@/lib/filters";
import type { Filters } from "@/lib/types";

type QueryOptions = {
  municipalities?: string[];
};

const STATUS_ORDER = new Map([
  ["approved", 0],
  ["pending", 1],
  ["rejected", 2],
  ["other", 3],
  ["unknown", 4]
]);

function sortStatuses(values: string[]) {
  return [...new Set(values)].sort((left, right) => {
    const leftRank = STATUS_ORDER.get(left.trim().toLowerCase()) ?? Number.MAX_SAFE_INTEGER;
    const rightRank = STATUS_ORDER.get(right.trim().toLowerCase()) ?? Number.MAX_SAFE_INTEGER;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return left.localeCompare(right, undefined, { sensitivity: "base" });
  });
}

function tokenizeMixedValue(value: string) {
  return value
    .trim()
    .toUpperCase()
    .split(/([0-9]+(?:\.[0-9]+)?)/)
    .filter(Boolean)
    .map((part) => {
      const numeric = Number(part);
      return Number.isNaN(numeric) ? part.trim() : numeric;
    });
}

function compareMixedNatural(left: string, right: string) {
  const leftTokens = tokenizeMixedValue(left);
  const rightTokens = tokenizeMixedValue(right);
  const length = Math.max(leftTokens.length, rightTokens.length);

  for (let index = 0; index < length; index += 1) {
    const leftToken = leftTokens[index];
    const rightToken = rightTokens[index];
    if (leftToken === undefined) {
      return -1;
    }
    if (rightToken === undefined) {
      return 1;
    }
    if (typeof leftToken === "number" && typeof rightToken === "number") {
      if (leftToken !== rightToken) {
        return leftToken - rightToken;
      }
      continue;
    }
    const comparison = String(leftToken).localeCompare(String(rightToken), undefined, {
      sensitivity: "base"
    });
    if (comparison !== 0) {
      return comparison;
    }
  }

  return left.localeCompare(right, undefined, {
    numeric: true,
    sensitivity: "base"
  });
}

function sortNatural(values: string[]) {
  return [...new Set(values)].sort(compareMixedNatural);
}

function withMunicipalityFilter(
  whereSql: string,
  values: Array<string | number | string[] | number[]>,
  municipalities: string[] | undefined
) {
  const clauses = whereSql ? [whereSql.replace(/^WHERE\s+/i, "")] : [];
  const nextValues = [...values];

  if (municipalities?.length) {
    clauses.unshift(`source_municipality = ANY($${nextValues.length + 1})`);
    nextValues.push(municipalities);
  }

  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    values: nextValues
  };
}

export async function getFilterOptions(options: QueryOptions = {}) {
  const client = await pool.connect();
  try {
    const municipalities = options.municipalities ?? ["MBJB"];
    const [statuses, years, planningBlocks, mukims] = await Promise.all([
      client.query(
        `SELECT DISTINCT public_display_status AS value
         FROM marts.public_applications
         WHERE source_municipality = ANY($1)
           AND public_display_status IS NOT NULL
         ORDER BY 1`,
        [municipalities]
      ),
      client.query(
        `SELECT DISTINCT COALESCE(approval_year, application_year) AS value
         FROM marts.public_applications
         WHERE source_municipality = ANY($1)
           AND COALESCE(approval_year, application_year) IS NOT NULL
         ORDER BY 1 DESC`,
        [municipalities]
      ),
      client.query(
        `SELECT DISTINCT planning_block AS value
         FROM marts.public_applications
         WHERE source_municipality = ANY($1)
           AND planning_block IS NOT NULL
           AND planning_block <> ''
         ORDER BY 1`,
        [municipalities]
      ),
      client.query(
        `SELECT DISTINCT mukim AS value
         FROM marts.public_applications
         WHERE source_municipality = ANY($1)
           AND mukim IS NOT NULL
           AND mukim <> ''
         ORDER BY 1`,
        [municipalities]
      )
    ]);

    return {
      statuses: sortStatuses(statuses.rows.map((row) => row.value as string)),
      years: years.rows.map((row) => Number(row.value)),
      planningBlocks: sortNatural(planningBlocks.rows.map((row) => row.value as string)),
      mukims: sortNatural(mukims.rows.map((row) => row.value as string))
    };
  } finally {
    client.release();
  }
}

export async function getOverview(filters: Filters, options: QueryOptions = {}) {
  const client = await pool.connect();
  try {
    const base = buildSqlFilter(filters);
    const { whereSql, values } = withMunicipalityFilter(base.whereSql, base.values, options.municipalities);
    const result = await client.query(
      `
      SELECT
        count(*)::int AS total_features,
        count(*) FILTER (WHERE public_display_status = 'approved')::int AS approved_features,
        count(*) FILTER (WHERE public_display_status = 'pending')::int AS pending_features,
        COALESCE(round(sum(area_acres)::numeric, 2), 0) AS total_area_acres
      FROM marts.public_applications
      ${whereSql}
      `,
      values
    );
    return result.rows[0];
  } finally {
    client.release();
  }
}

export async function getDistribution(
  filters: Filters,
  field: "layer_type" | "public_display_status" | "year",
  options: QueryOptions = {}
) {
  const client = await pool.connect();
  try {
    const base = buildSqlFilter(filters);
    const { whereSql, values } = withMunicipalityFilter(base.whereSql, base.values, options.municipalities);
    const selectExpr =
      field === "year" ? "COALESCE(approval_year, application_year)" : field;
    const rows = await client.query(
      `
      SELECT ${selectExpr} AS bucket, count(*)::int AS feature_count
      FROM marts.public_applications
      ${whereSql}
      GROUP BY 1
      ORDER BY 2 DESC, 1
      `,
      values
    );
    return rows.rows.map((row) => ({
      key: row.bucket,
      label: row.bucket == null ? "Unknown" : String(row.bucket),
      count: Number(row.feature_count)
    }));
  } finally {
    client.release();
  }
}
