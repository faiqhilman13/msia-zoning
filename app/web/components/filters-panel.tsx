import clsx from "clsx";

import { getLayerOptions } from "@/lib/filters";
import type { FilterOptions, Filters, LayerType, MunicipalityCode } from "@/lib/types";

type Props = {
  municipality: MunicipalityCode;
  filters: Filters;
  options: FilterOptions;
  showPrimaryContext: boolean;
  showBoundary: boolean;
  onToggleLayer: (layer: LayerType) => void;
  onToggleValue: (
    kind: "statuses" | "years" | "planningBlocks" | "mukims",
    value: string | number
  ) => void;
  onReset: () => void;
  onTogglePrimaryContext: () => void;
  onToggleBoundary: () => void;
};

function toggleClass(active: boolean, extra?: string) {
  return clsx("chip", extra, active && "active");
}

export function FiltersPanel({
  municipality,
  filters,
  options,
  showPrimaryContext,
  showBoundary,
  onToggleLayer,
  onToggleValue,
  onReset,
  onTogglePrimaryContext,
  onToggleBoundary
}: Props) {
  const layerOptions = getLayerOptions(municipality);
  const heroTitle =
    municipality === "MBPJ" ? "Petaling Jaya Development Register" : "Johor Bahru Development Map";
  const heroBody =
    municipality === "MBPJ"
      ? "Public MBPJ SmartDev register rows are searchable alongside MBPJ context geometry from the municipal ArcGIS service."
      : "Public MBJB development polygons served from PostGIS tiles, with safe public fields only.";
  const primaryContextLabel = municipality === "MBPJ" ? "Official buildings" : "Planning blocks";
  const boundaryLabel = municipality === "MBPJ" ? "MBPJ boundary" : "MBJB boundary";

  return (
    <section className="panel left-panel">
      <div className="hero-copy">
        <p className="eyebrow">{municipality}</p>
        <h1>{heroTitle}</h1>
        <p>{heroBody}</p>
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

      {options.planningBlocks.length ? (
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
      ) : null}

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
          <button className={toggleClass(showPrimaryContext)} type="button" onClick={onTogglePrimaryContext}>
            {primaryContextLabel}
          </button>
          <button className={toggleClass(showBoundary)} type="button" onClick={onToggleBoundary}>
            {boundaryLabel}
          </button>
        </div>
      </div>

      <button className="clear-button" type="button" onClick={onReset}>
        Clear all filters
      </button>
    </section>
  );
}
