import clsx from "clsx";

import type { FilterOptions, Filters, LayerType } from "@/lib/types";

type Props = {
  filters: Filters;
  options: FilterOptions;
  showPlanningBlocks: boolean;
  showBoundary: boolean;
  onToggleLayer: (layer: LayerType) => void;
  onToggleValue: (
    kind: "statuses" | "years" | "planningBlocks" | "mukims",
    value: string | number
  ) => void;
  onReset: () => void;
  onTogglePlanningBlocks: () => void;
  onToggleBoundary: () => void;
};

function toggleClass(active: boolean, extra?: string) {
  return clsx("chip", extra, active && "active");
}

export function FiltersPanel({
  filters,
  options,
  showPlanningBlocks,
  showBoundary,
  onToggleLayer,
  onToggleValue,
  onReset,
  onTogglePlanningBlocks,
  onToggleBoundary
}: Props) {
  const layerOptions: Array<{ key: LayerType; label: string }> = [
    { key: "kebenaran_merancang", label: "Kebenaran Merancang" },
    { key: "pelan_bangunan", label: "Pelan Bangunan" },
    { key: "kerja_tanah", label: "Kerja Tanah" }
  ];

  return (
    <section className="panel left-panel">
      <div className="hero-copy">
        <p className="eyebrow">MBJB MVP</p>
        <h1>Johor Bahru Development Map</h1>
        <p>
          Public MBJB development polygons served from PostGIS tiles, with safe public fields only.
        </p>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Development Layers</p>
        <div className="chip-grid">
          {layerOptions.map((layer) => {
            const active = filters.layerTypes.includes(layer.key);
            return (
              <button
                key={layer.key}
                className={toggleClass(active, `layer-${layer.key}`)}
                onClick={() => onToggleLayer(layer.key)}
                type="button"
              >
                {layer.label}
              </button>
            );
          })}
        </div>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Status</p>
        <div className="chip-grid">
          {options.statuses.map((status) => {
            const active = filters.statuses.includes(status);
            return (
              <button
                key={status}
                className={toggleClass(active)}
                onClick={() => onToggleValue("statuses", status)}
                type="button"
              >
                {status}
              </button>
            );
          })}
        </div>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Year</p>
        <div className="chip-grid">
          {options.years.map((year) => {
            const active = filters.years.includes(year);
            return (
              <button
                key={year}
                className={toggleClass(active)}
                onClick={() => onToggleValue("years", year)}
                type="button"
              >
                {year}
              </button>
            );
          })}
        </div>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Planning Block</p>
        <div className="chip-grid">
          {options.planningBlocks.map((item) => {
            const active = filters.planningBlocks.includes(item);
            return (
              <button
                key={item}
                className={toggleClass(active)}
                onClick={() => onToggleValue("planningBlocks", item)}
                type="button"
              >
                {item}
              </button>
            );
          })}
        </div>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Mukim</p>
        <div className="chip-grid">
          {options.mukims.map((item) => {
            const active = filters.mukims.includes(item);
            return (
              <button
                key={item}
                className={toggleClass(active)}
                onClick={() => onToggleValue("mukims", item)}
                type="button"
              >
                {item}
              </button>
            );
          })}
        </div>
      </div>

      <div className="filter-group">
        <p className="eyebrow">Context Overlays</p>
        <div className="chip-grid">
          <button className={toggleClass(showPlanningBlocks)} type="button" onClick={onTogglePlanningBlocks}>
            Planning blocks
          </button>
          <button className={toggleClass(showBoundary)} type="button" onClick={onToggleBoundary}>
            MBJB boundary
          </button>
        </div>
      </div>

      <button className="clear-button" type="button" onClick={onReset}>
        Clear all filters
      </button>
    </section>
  );
}
