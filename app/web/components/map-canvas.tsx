"use client";

import { useEffect, useRef, useState } from "react";

import { MVTLayer } from "@deck.gl/geo-layers";
import { GeoJsonLayer, ScatterplotLayer, TextLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import maplibregl, { type Map } from "maplibre-gl";

import { boundariesUrl, contextBuildingsUrl, layerColors, planningBlocksUrl } from "@/lib/map";
import type { HoverState, LayerType, MunicipalityCode, PlanningBlockPoint } from "@/lib/types";

type FocusPoint = {
  lon: number;
  lat: number;
  zoom?: number;
};

type Props = {
  municipality: MunicipalityCode;
  tilesUrl: string | null;
  filterQuery: string;
  showPrimaryContext: boolean;
  showBoundary: boolean;
  selectedApplicationId: string | null;
  selectedPlanningBlockId: string | null;
  focusPoint: FocusPoint | null;
  onHover: (value: HoverState | null) => void;
  onSelectApplication: (applicationId: string) => void;
  onSelectPlanningBlock: (planningBlockId: string) => void;
};

function getLayerColor(layerType: string) {
  const fallback: [number, number, number] = [88, 98, 94];
  return layerColors[layerType as LayerType] ?? fallback;
}

function darkenColor([r, g, b]: [number, number, number], factor = 0.28): [number, number, number] {
  return [
    Math.max(0, Math.round(r * (1 - factor))),
    Math.max(0, Math.round(g * (1 - factor))),
    Math.max(0, Math.round(b * (1 - factor)))
  ];
}

function lightenColor([r, g, b]: [number, number, number], factor = 0.18): [number, number, number] {
  return [
    Math.min(255, Math.round(r + (255 - r) * factor)),
    Math.min(255, Math.round(g + (255 - g) * factor)),
    Math.min(255, Math.round(b + (255 - b) * factor))
  ];
}

export function MapCanvas({
  municipality,
  tilesUrl,
  filterQuery,
  showPrimaryContext,
  showBoundary,
  selectedApplicationId,
  selectedPlanningBlockId,
  focusPoint,
  onHover,
  onSelectApplication,
  onSelectPlanningBlock
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const overlayRef = useRef<MapboxOverlay | null>(null);
  const [planningBlockPoints, setPlanningBlockPoints] = useState<PlanningBlockPoint[]>([]);

  useEffect(() => {
    if (municipality !== "MBJB" || !showPrimaryContext) {
      setPlanningBlockPoints([]);
      return;
    }

    const controller = new AbortController();
    const suffix = filterQuery ? `?${filterQuery}` : "";
    fetch(`/api/v1/context/planning-blocks${suffix}`, { signal: controller.signal })
      .then((response) => response.json())
      .then((payload) => setPlanningBlockPoints(payload))
      .catch(() => setPlanningBlockPoints([]));

    return () => controller.abort();
  }, [filterQuery, municipality, showPrimaryContext]);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }

    const defaultLon =
      municipality === "MBPJ"
        ? Number(process.env.NEXT_PUBLIC_MBPJ_DEFAULT_LON ?? 101.6237)
        : Number(process.env.NEXT_PUBLIC_DEFAULT_LON ?? 103.7414);
    const defaultLat =
      municipality === "MBPJ"
        ? Number(process.env.NEXT_PUBLIC_MBPJ_DEFAULT_LAT ?? 3.1073)
        : Number(process.env.NEXT_PUBLIC_DEFAULT_LAT ?? 1.4927);
    const defaultZoom =
      municipality === "MBPJ"
        ? Number(process.env.NEXT_PUBLIC_MBPJ_DEFAULT_ZOOM ?? 12)
        : Number(process.env.NEXT_PUBLIC_DEFAULT_ZOOM ?? 11);

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: process.env.NEXT_PUBLIC_MAP_STYLE_URL ?? "https://demotiles.maplibre.org/style.json",
      center: [defaultLon, defaultLat],
      zoom: defaultZoom,
      pitch: 34,
      bearing: -7,
      attributionControl: false
    });

    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");
    map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-right");

    const overlay = new MapboxOverlay({ layers: [] });
    map.addControl(overlay);

    mapRef.current = map;
    overlayRef.current = overlay;

    return () => {
      overlay.finalize();
      map.remove();
      overlayRef.current = null;
      mapRef.current = null;
    };
  }, [municipality]);

  useEffect(() => {
    if (!mapRef.current || !focusPoint) {
      return;
    }
    mapRef.current.flyTo({
      center: [focusPoint.lon, focusPoint.lat],
      zoom: focusPoint.zoom ?? 15,
      speed: 0.8,
      essential: true
    });
  }, [focusPoint]);

  useEffect(() => {
    if (!overlayRef.current) {
      return;
    }

    const developmentLayer =
      municipality === "MBJB" && tilesUrl
        ? new MVTLayer({
            id: `development-${tilesUrl}`,
            data: tilesUrl,
            minZoom: 0,
            maxZoom: 22,
            pickable: true,
            autoHighlight: true,
            highlightColor: [255, 255, 255, 70],
            onHover: (info) => {
              const properties = (info.object as { properties?: Record<string, unknown> } | null)?.properties;
              if (!properties || typeof properties.application_id !== "string") {
                onHover(null);
                return;
              }
              onHover({
                x: info.x ?? 0,
                y: info.y ?? 0,
                applicationId: properties.application_id,
                referenceNo: (properties.reference_no as string | null) ?? null,
                title: String(properties.public_display_title ?? "MBJB feature"),
                layerType: String(properties.layer_type ?? "unknown"),
                status: String(properties.public_display_status ?? "unknown")
              });
            },
            onClick: (info) => {
              const properties = (info.object as { properties?: Record<string, unknown> } | null)?.properties;
              if (properties && typeof properties.application_id === "string") {
                onSelectApplication(properties.application_id);
              }
            },
            renderSubLayers: (props) =>
              new GeoJsonLayer(props, {
                data: props.data,
                extruded: true,
                filled: true,
                stroked: true,
                lineWidthMinPixels: 1.8,
                getLineColor: (feature) => {
                  const properties = feature.properties as Record<string, unknown>;
                  const [r, g, b] = darkenColor(getLayerColor(String(properties.layer_type ?? "")));
                  if (properties.application_id === selectedApplicationId) {
                    return [250, 247, 240, 255];
                  }
                  return [r, g, b, 235];
                },
                getFillColor: (feature) => {
                  const properties = feature.properties as Record<string, unknown>;
                  const baseColor = getLayerColor(String(properties.layer_type ?? ""));
                  if (properties.application_id === selectedApplicationId) {
                    const [r, g, b] = lightenColor(baseColor, 0.36);
                    return [r, g, b, 255];
                  }
                  const [r, g, b] = baseColor;
                  return [r, g, b, 215];
                },
                getElevation: (feature) => {
                  const properties = feature.properties as Record<string, unknown>;
                  const areaM2 = Number(properties.area_m2 ?? 0);
                  const base =
                    properties.layer_type === "pelan_bangunan"
                      ? 74
                      : properties.layer_type === "kebenaran_merancang"
                        ? 56
                        : 42;
                  return Math.max(base, Math.min(250, areaM2 / 52));
                },
                material: {
                  ambient: 0.42,
                  diffuse: 0.56,
                  shininess: 20,
                  specularColor: [255, 248, 232]
                }
              })
          })
        : null;

    const planningLayer = new MVTLayer({
      id: "planning-blocks",
      data: planningBlocksUrl(),
      visible: municipality === "MBJB" && showPrimaryContext,
      pickable: false,
      renderSubLayers: (props) =>
        new GeoJsonLayer(props, {
          data: props.data,
          filled: false,
          stroked: true,
          lineWidthMinPixels: 1,
          getLineColor: [116, 124, 158, 115]
        })
    });

    const planningBlockDotsLayer = new ScatterplotLayer<PlanningBlockPoint>({
      id: "planning-block-dots",
      data: planningBlockPoints,
      visible: municipality === "MBJB" && showPrimaryContext,
      pickable: true,
      radiusUnits: "pixels",
      stroked: true,
      filled: true,
      lineWidthMinPixels: 2,
      getPosition: (item) => [item.centroidLon, item.centroidLat],
      getRadius: (item) =>
        item.planningBlockId === selectedPlanningBlockId
          ? 10
          : Math.max(4, Math.min(9, 4 + item.featureCount / 18)),
      getFillColor: (item) =>
        item.planningBlockId === selectedPlanningBlockId ? [18, 33, 28, 235] : [248, 243, 230, 210],
      getLineColor: [116, 124, 158, 210],
      onClick: (info) => {
        const point = info.object;
        if (point) {
          onSelectPlanningBlock(point.planningBlockId);
        }
      }
    });

    const planningBlockLabelsLayer = new TextLayer<PlanningBlockPoint>({
      id: "planning-block-labels",
      data: planningBlockPoints,
      visible: municipality === "MBJB" && showPrimaryContext,
      pickable: false,
      getPosition: (item) => [item.centroidLon, item.centroidLat],
      getText: (item) => item.planningBlock,
      getSize: (item) => (item.planningBlockId === selectedPlanningBlockId ? 15 : 11.5),
      getColor: (item) =>
        item.planningBlockId === selectedPlanningBlockId
          ? [18, 33, 28, 255]
          : [73, 81, 111, 235],
      getPixelOffset: [0, -16],
      fontFamily: "Georgia, serif",
      background: true,
      getBackgroundColor: [249, 244, 233, 196],
      getBorderColor: [116, 124, 158, 86],
      getBorderWidth: 1
    });

    const contextBuildingsLayer = new MVTLayer({
      id: "mbpj-context-buildings",
      data: contextBuildingsUrl(),
      visible: municipality === "MBPJ" && showPrimaryContext,
      pickable: false,
      renderSubLayers: (props) =>
        new GeoJsonLayer(props, {
          data: props.data,
          extruded: false,
          filled: true,
          stroked: true,
          lineWidthMinPixels: 1.1,
          getLineColor: [110, 71, 38, 220],
          getFillColor: [170, 108, 56, 110]
        })
    });

    const boundaryLayer = new MVTLayer({
      id: `${municipality.toLowerCase()}-boundary`,
      data: boundariesUrl(municipality),
      visible: showBoundary,
      pickable: false,
      renderSubLayers: (props) =>
        new GeoJsonLayer(props, {
          data: props.data,
          filled: false,
          stroked: true,
          lineWidthMinPixels: 2,
          getLineColor: [18, 33, 28, 235]
        })
    });

    overlayRef.current.setProps({
      layers: [
        planningLayer,
        contextBuildingsLayer,
        boundaryLayer,
        developmentLayer,
        planningBlockDotsLayer,
        planningBlockLabelsLayer
      ].filter(Boolean)
    });
  }, [
    municipality,
    onHover,
    onSelectApplication,
    onSelectPlanningBlock,
    planningBlockPoints,
    selectedApplicationId,
    selectedPlanningBlockId,
    showBoundary,
    showPrimaryContext,
    tilesUrl
  ]);

  return (
    <div className="map-panel">
      <div ref={containerRef} className="map-canvas" />
      <div className="map-overlay">
        {municipality === "MBPJ"
          ? "MBPJ SmartDev rows stay text-first. The map shows MBPJ official-building context polygons and the municipal boundary from the public ArcGIS service."
          : "MBJB polygons are rendered from PostGIS vector tiles. Planning block labels and dots sit on top of the GeoJB planning block overlay; click a dot to inspect planning block context."}
      </div>
    </div>
  );
}
