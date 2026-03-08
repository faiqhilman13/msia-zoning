"use client";

import { startTransition, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

import { DetailDrawer } from "@/components/detail-drawer";
import { FiltersPanel } from "@/components/filters-panel";
import { Legend } from "@/components/legend";
import { MapCanvas } from "@/components/map-canvas";
import { SearchBox } from "@/components/search-box";
import { StatsBar } from "@/components/stats-bar";
import { getDefaultFilters } from "@/lib/filters";
import { buildTilesUrl, getDefaultViewport } from "@/lib/map";
import type {
  FeatureDetail,
  FilterOptions,
  Filters,
  HoverState,
  MunicipalityCode,
  OverviewStats,
  SearchResult
} from "@/lib/types";

type DistributionRow = {
  key: string | number | null;
  label: string;
  count: number;
};

type SelectedTarget =
  | { kind: "application"; id: string }
  | { kind: "planning_block"; id: string }
  | null;

function defaultFocusPoint(municipality: MunicipalityCode) {
  return getDefaultViewport(municipality);
}

function parseSelectedMunicipality(searchParams: { get(name: string): string | null }): MunicipalityCode {
  return searchParams.get("municipality")?.toUpperCase() === "MBPJ" ? "MBPJ" : "MBJB";
}

function filtersToQuery(filters: Filters, municipality: MunicipalityCode) {
  const query = new URLSearchParams();
  query.set("municipality", municipality);

  const defaultLayerTypes = getDefaultFilters(municipality).layerTypes;
  const includeLayers =
    filters.layerTypes.length !== defaultLayerTypes.length ||
    filters.layerTypes.some((value) => !defaultLayerTypes.includes(value));

  if (includeLayers) {
    filters.layerTypes.forEach((value) => query.append("layer", value));
  }
  filters.statuses.forEach((value) => query.append("status", value));
  filters.years.forEach((value) => query.append("year", String(value)));
  filters.planningBlocks.forEach((value) => query.append("planningBlock", value));
  filters.mukims.forEach((value) => query.append("mukim", value));
  return query.toString();
}

export function MapShell() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const municipality = parseSelectedMunicipality(searchParams);
  const [selectedTarget, setSelectedTarget] = useState<SelectedTarget>(null);
  const [filters, setFilters] = useState<Filters>(() => getDefaultFilters(municipality));
  const [options, setOptions] = useState<FilterOptions>({
    statuses: [],
    years: [],
    planningBlocks: [],
    mukims: []
  });
  const [overview, setOverview] = useState<OverviewStats | null>(null);
  const [byLayer, setByLayer] = useState<DistributionRow[]>([]);
  const [selectedFeature, setSelectedFeature] = useState<FeatureDetail | null>(null);
  const [hoverState, setHoverState] = useState<HoverState | null>(null);
  const [showPrimaryContext, setShowPrimaryContext] = useState(true);
  const [showBoundary, setShowBoundary] = useState(true);
  const [focusPoint, setFocusPoint] = useState<{ lon: number; lat: number; zoom?: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setFilters(getDefaultFilters(municipality));
    setSelectedTarget(null);
    setSelectedFeature(null);
    setHoverState(null);
    setShowPrimaryContext(true);
    setShowBoundary(true);
    setFocusPoint(defaultFocusPoint(municipality));
  }, [municipality]);

  useEffect(() => {
    const controller = new AbortController();
    const query = new URLSearchParams({ municipality }).toString();
    setLoading(true);
    startTransition(() => {
      fetch(`/api/v1/metadata/filter-options?${query}`, { signal: controller.signal })
        .then((res) => res.json())
        .then((filterOptions) => {
          setOptions(filterOptions);
          setError(null);
        })
        .catch(() => {
          setError(`Could not load ${municipality} metadata.`);
        })
        .finally(() => setLoading(false));
    });

    return () => controller.abort();
  }, [municipality]);

  useEffect(() => {
    const controller = new AbortController();
    const query = filtersToQuery(filters, municipality);
    const suffix = query ? `?${query}` : "";
    startTransition(() => {
      Promise.all([
        fetch(`/api/v1/stats/overview${suffix}`, { signal: controller.signal }).then((res) => res.json()),
        fetch(`/api/v1/stats/by-layer${suffix}`, { signal: controller.signal }).then((res) => res.json())
      ])
        .then(([overviewStats, layerRows]) => {
          setOverview(overviewStats);
          setByLayer(layerRows);
          setError(null);
        })
        .catch(() => {
          setError(`Could not refresh ${municipality} stats.`);
        });
    });
    return () => controller.abort();
  }, [filters, municipality]);

  useEffect(() => {
    if (!selectedTarget) {
      setSelectedFeature(null);
      return;
    }

    const controller = new AbortController();
    const route =
      selectedTarget.kind === "application"
        ? `/api/v1/features/${selectedTarget.id}`
        : `/api/v1/context/planning-blocks/${selectedTarget.id}`;

    fetch(route, { signal: controller.signal })
      .then((res) => res.json())
      .then((payload) => {
        setSelectedFeature(payload);
        if (payload.centroidLon != null && payload.centroidLat != null) {
          setFocusPoint({
            lon: payload.centroidLon,
            lat: payload.centroidLat,
            zoom: payload.kind === "planning_block" ? 13.3 : 15
          });
        }
      })
      .catch(() => {
        setSelectedFeature(null);
      });

    return () => controller.abort();
  }, [selectedTarget]);

  const tilesUrl = buildTilesUrl(filters, municipality);
  const filterQuery = filtersToQuery(filters, municipality);
  const selectedApplicationId = selectedTarget?.kind === "application" ? selectedTarget.id : null;
  const selectedPlanningBlockId =
    municipality === "MBJB" && selectedTarget?.kind === "planning_block" ? selectedTarget.id : null;

  return (
    <main className="app-shell">
      <StatsBar overview={overview} byLayer={byLayer} />

      <section className="sidebar-stack">
        <section className="panel search-panel">
          <div className="municipality-switch">
            {(["MBJB", "MBPJ"] as MunicipalityCode[]).map((code) => {
              const active = code === municipality;
              return (
                <button
                  key={code}
                  className={`chip ${active ? "active" : ""}`}
                  type="button"
                  onClick={() => router.replace(`/?municipality=${code}`)}
                >
                  {code}
                </button>
              );
            })}
          </div>
          <SearchBox
            key={municipality}
            municipality={municipality}
            onSelect={(result: SearchResult) => {
              setSelectedTarget({ kind: "application", id: result.applicationId });
              if (result.centroidLon != null && result.centroidLat != null) {
                setFocusPoint({ lon: result.centroidLon, lat: result.centroidLat, zoom: 15 });
              }
            }}
          />
        </section>
        <FiltersPanel
          municipality={municipality}
          filters={filters}
          options={options}
          showPrimaryContext={showPrimaryContext}
          showBoundary={showBoundary}
          onToggleLayer={(layer) =>
            setFilters((current) => {
              const next = current.layerTypes.includes(layer)
                ? current.layerTypes.filter((value) => value !== layer)
                : [...current.layerTypes, layer];
              return {
                ...current,
                layerTypes: next.length ? next : current.layerTypes
              };
            })
          }
          onToggleValue={(kind, value) =>
            setFilters((current) => {
              const currentValues = current[kind];
              const exists = currentValues.includes(value as never);
              return {
                ...current,
                [kind]: exists
                  ? currentValues.filter((item) => item !== value)
                  : [...currentValues, value]
              };
            })
          }
          onReset={() => setFilters(getDefaultFilters(municipality))}
          onTogglePrimaryContext={() => setShowPrimaryContext((value) => !value)}
          onToggleBoundary={() => setShowBoundary((value) => !value)}
        />
      </section>

      <section className="map-stage">
        <MapCanvas
          municipality={municipality}
          tilesUrl={tilesUrl}
          filterQuery={filterQuery}
          showPrimaryContext={showPrimaryContext}
          showBoundary={showBoundary}
          selectedApplicationId={selectedApplicationId}
          selectedPlanningBlockId={selectedPlanningBlockId}
          focusPoint={focusPoint}
          onHover={setHoverState}
          onSelectApplication={(applicationId) =>
            setSelectedTarget({ kind: "application", id: applicationId })
          }
          onSelectPlanningBlock={(planningBlockId) =>
            setSelectedTarget({ kind: "planning_block", id: planningBlockId })
          }
        />
        {loading ? <div className="panel muted">Loading {municipality} map metadata...</div> : null}
        {error ? <div className="panel muted">{error}</div> : null}
      </section>

      <aside className="right-panel">
        <DetailDrawer detail={selectedFeature} />
        <Legend municipality={municipality} />
        <section className="detail-card">
          <p className="eyebrow">Context</p>
          <p className="muted">
            {municipality === "MBPJ"
              ? "MBPJ map context comes from the public ArcGIS official-buildings layer plus the municipal boundary. Search and stats still query the canonical application tables, while SmartDev project rows remain geometry-null."
              : "Planning blocks come from MBJB Rancangan Tempatan layer 0. Boundary context uses Sempadan MBJB Merge. Search and stats query PostGIS directly; map rendering uses pg_tileserv vector tiles."}
          </p>
          <a className="link-button" href="/sources">
            Source attribution
          </a>
        </section>
      </aside>

      {hoverState ? (
        <div className="tooltip" style={{ left: hoverState.x, top: hoverState.y }}>
          <div className="eyebrow">{hoverState.layerType.replaceAll("_", " ")}</div>
          <strong>{hoverState.referenceNo ?? hoverState.title}</strong>
          <div className="muted">{hoverState.title}</div>
          <div className={`status status-${hoverState.status}`}>{hoverState.status}</div>
        </div>
      ) : null}
    </main>
  );
}
