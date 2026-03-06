from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import geopandas as gpd
import pandas as pd
from shapely import force_2d
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid

from malaysia_permits_map.config import AppConfig
from malaysia_permits_map.etl.arcgis_client import ArcGISClient, write_json
from malaysia_permits_map.models.arcgis import ArcGISArtifact, ArcGISLayerConfig
from malaysia_permits_map.utils.paths import ensure_directory
from malaysia_permits_map.utils.text import (
    clean_whitespace,
    derive_public_title,
    normalize_mukim,
    normalize_planning_block,
    normalize_status,
    snake_case,
)


SOURCE_SYSTEM = "mbjb_geojb"
MUNICIPALITY = "MBJB"
NAMESPACE = uuid.UUID("8db9f5ff-9d9d-4f6e-a5f5-7792a4d2d6b0")

DEVELOPMENT_LAYERS: list[ArcGISLayerConfig] = [
    ArcGISLayerConfig(
        slug="kebenaran_merancang",
        label="Kebenaran Merancang",
        service_name="MaklumatPembangunan",
        layer_id=3,
        application_type="Kebenaran Merancang",
        field_map={
            "No_Fail": "reference_no",
            "No_Fail_Perancang": "reference_no_alt",
            "Tajuk_Fail": "title",
            "STATUS_PERMOHONAN_SEMASA": "current_status",
            "Tahun_Mohon": "application_year",
            "TAHUN_LULUS": "approval_year",
            "NO_LOT": "lot_no",
            "MUKIM": "mukim",
            "BLOK_PERANCANGAN": "planning_block",
            "Zoning_Perancang": "zoning_name",
            "PEMILIK": "owner_name_raw",
            "PEMAJU": "developer_name",
            "PERUNDING": "consultant_name",
            "PEMEGANG_PA": "proxy_holder_name",
            "LUAS_EKAR": "site_area_acres",
            "JK_OSC_1": "meeting_date_1",
            "Keputusan_OSC_1": "meeting_decision_1",
            "JK_OSC_2": "meeting_date_2",
            "Keputusan_OSC_2": "meeting_decision_2",
            "JK_OSC_3": "meeting_date_3",
            "Keputusan_OSC_3": "meeting_decision_3",
            "TARIKH_MESY_4": "meeting_date_4",
            "KEPUTUSAN_MESY_4": "meeting_decision_4",
        },
    ),
    ArcGISLayerConfig(
        slug="pelan_bangunan",
        label="Pelan Bangunan",
        service_name="MaklumatPembangunan",
        layer_id=1,
        application_type="Pelan Bangunan",
        field_map={
            "No_Fail": "reference_no",
            "No_Fail_Bangunan": "reference_no_alt",
            "Tajuk_Fail": "title",
            "Status_Semasa": "current_status",
            "Tahun_Mohon": "application_year",
            "Tahun_Lulus": "approval_year",
            "JK_OSC_1": "meeting_date_1",
            "Keputusan_OSC_1": "meeting_decision_1",
            "JK_OSC_2": "meeting_date_2",
            "Keputusan_OSC_2": "meeting_decision_2",
            "JK_OSC_3": "meeting_date_3",
            "Keputusan_OSC_3": "meeting_decision_3",
            "PSP_Bangunan": "consultant_name",
        },
    ),
    ArcGISLayerConfig(
        slug="kerja_tanah",
        label="Kerja Tanah",
        service_name="MaklumatPembangunan",
        layer_id=4,
        application_type="Kerja Tanah",
        field_map={
            "No_Fail": "reference_no",
            "Tajuk_Fail": "title",
            "Status_Semasa": "current_status",
            "Tahun_Mohon": "application_year",
            "Tahun_Lulus": "approval_year",
            "JK_OSC_1": "meeting_date_1",
            "Keputusan_OSC_1": "meeting_decision_1",
            "JK_OSC_2": "meeting_date_2",
            "Keputusan_OSC_2": "meeting_decision_2",
            "JK_OSC_3": "meeting_date_3",
            "Keputusan_OSC_3": "meeting_decision_3",
        },
    ),
]

CONTEXT_LAYERS: list[ArcGISLayerConfig] = [
    ArcGISLayerConfig(
        slug="mbjb_boundary",
        label="Sempadan MBJB",
        service_name="Sempadan_MBJB_Merge",
        layer_id=1,
        application_type="Boundary",
        kind="context",
    ),
    ArcGISLayerConfig(
        slug="planning_blocks",
        label="Rancangan Tempatan Planning Blocks",
        service_name="RancanganTempatan",
        layer_id=0,
        application_type="Planning Block",
        kind="context",
    ),
    ArcGISLayerConfig(
        slug="mukim",
        label="Mukim",
        service_name="Mukim_161025",
        layer_id=0,
        application_type="Mukim",
        kind="context",
    ),
]

EXPECTED_BASELINE_COUNTS = {
    "pelan_bangunan": 618,
    "kebenaran_merancang": 310,
    "kerja_tanah": 190,
}

EXPECTED_SOURCE_EXPORT_FORMATS = ("geojson", "csv", "shapefile")
EXPECTED_SOURCE_GEOMETRY_TYPE = "esriGeometryPolygon"


@dataclass
class PipelineRun:
    ingest_run_id: str
    started_at: datetime
    raw_root: Path
    stage_root: Path
    artifacts: list[ArcGISArtifact]
    observed_counts: dict[str, int]

    @property
    def manifest_path(self) -> Path:
        return self.raw_root / "manifest.json"


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def chunked(items: list[int], chunk_size: int) -> Iterable[list[int]]:
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def make_run(config: AppConfig, run_label: str | None = None) -> PipelineRun:
    started_at = utc_now()
    run_stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    if run_label:
        run_stamp = f"{run_stamp}_{snake_case(run_label)}"
    raw_root = ensure_directory(config.data_raw_dir / run_stamp)
    stage_root = ensure_directory(config.data_stage_dir / run_stamp)
    return PipelineRun(
        ingest_run_id=str(uuid.uuid4()),
        started_at=started_at,
        raw_root=raw_root,
        stage_root=stage_root,
        artifacts=[],
        observed_counts={},
    )


def _serialize_record_hash(properties: dict[str, Any], geometry_wkb_hex: str) -> str:
    payload = json.dumps(properties, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload + geometry_wkb_hex.encode("utf-8")).hexdigest()


def _ensure_multipolygon(geometry: Polygon | MultiPolygon) -> MultiPolygon:
    if isinstance(geometry, MultiPolygon):
        return geometry
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    raise TypeError(f"Unsupported geometry type: {geometry.geom_type}")


def _arcgis_ms_to_timestamp(value: Any) -> datetime | None:
    if value in (None, "", " "):
        return None
    numeric = float(value)
    if math.isnan(numeric):
        return None
    return datetime.fromtimestamp(numeric / 1000.0, tz=UTC)


def _int_or_none(value: Any) -> int | None:
    if value in (None, "", " "):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, "", " "):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _stable_application_id(layer_slug: str, object_id: int) -> str:
    natural_key = f"{SOURCE_SYSTEM}|{layer_slug}|{object_id}"
    return str(uuid.uuid5(NAMESPACE, natural_key))


def _series_or_default(frame: gpd.GeoDataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([None] * len(frame), index=frame.index)


def ingest_layer(client: ArcGISClient, run: PipelineRun, config: AppConfig, layer: ArcGISLayerConfig) -> None:
    layer_root = ensure_directory(run.raw_root / layer.slug)
    metadata = client.fetch_layer_metadata(layer)
    count = client.fetch_count(layer)
    ids = client.fetch_ids(layer)
    run.observed_counts[layer.slug] = count
    run.artifacts.append(write_json(layer_root / "layer-metadata.json", metadata))
    run.artifacts.append(write_json(layer_root / "count.json", {"count": count}))
    run.artifacts.append(write_json(layer_root / "ids.json", {"object_ids": ids}))
    batch_files: list[str] = []
    for batch_index, object_ids in enumerate(chunked(ids, config.mbjb_max_batch_size), start=1):
        content = client.fetch_geojson_batch(layer, object_ids)
        batch_name = f"batch-{batch_index:04d}.geojson"
        artifact = client.write_bytes(layer_root / batch_name, content)
        batch_files.append(batch_name)
        run.artifacts.append(artifact)
    run.artifacts.append(
        write_json(
            layer_root / "manifest.json",
            {
                "layer": layer.slug,
                "label": layer.label,
                "count": count,
                "batch_files": batch_files,
                "object_id_field": metadata.get("objectIdField", layer.object_id_field),
            },
        )
    )


def _read_geojson_batches(layer_root: Path) -> gpd.GeoDataFrame:
    gdfs: list[gpd.GeoDataFrame] = []
    for batch_path in sorted(layer_root.glob("batch-*.geojson")):
        gdf = gpd.read_file(batch_path)
        if not gdf.empty:
            gdfs.append(gdf)
    if not gdfs:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    return pd.concat(gdfs, ignore_index=True)


def _normalize_geometry(series: gpd.GeoSeries) -> gpd.GeoSeries:
    normalized = []
    for geom in series:
        if geom is None:
            normalized.append(None)
            continue
        fixed = make_valid(force_2d(geom))
        if fixed.geom_type == "GeometryCollection":
            polygons = [part for part in fixed.geoms if part.geom_type in {"Polygon", "MultiPolygon"}]
            if not polygons:
                normalized.append(None)
                continue
            pieces = []
            for part in polygons:
                pieces.extend(list(part.geoms) if hasattr(part, "geoms") else [part])
            fixed = MultiPolygon(pieces)
        normalized.append(_ensure_multipolygon(fixed))
    return gpd.GeoSeries(normalized, crs=series.crs)


def normalize_context_layers(run: PipelineRun) -> dict[str, gpd.GeoDataFrame]:
    result: dict[str, gpd.GeoDataFrame] = {}
    for layer in CONTEXT_LAYERS:
        gdf = _read_geojson_batches(run.raw_root / layer.slug)
        if gdf.empty:
            continue
        gdf.columns = [snake_case(column) for column in gdf.columns]
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        if gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)
        gdf["geometry"] = _normalize_geometry(gdf.geometry)
        gdf = gdf.loc[gdf.geometry.notnull()].copy()
        if layer.slug == "planning_blocks":
            if "bpk" in gdf.columns:
                gdf["bpk"] = gdf["bpk"].map(normalize_planning_block)
            if "nama_bpk" in gdf.columns:
                gdf["nama_bpk"] = gdf["nama_bpk"].map(clean_whitespace)
            if "mukim" in gdf.columns:
                gdf["mukim"] = gdf["mukim"].map(normalize_mukim)
        if layer.slug == "mukim" and "nama" in gdf.columns:
            gdf["nama"] = gdf["nama"].map(normalize_mukim)
        gdf.to_parquet(run.stage_root / f"{layer.slug}.parquet", index=False)
        result[layer.slug] = gdf
    return result


def _spatial_enrich(
    features: gpd.GeoDataFrame, context_layers: dict[str, gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    enriched = features.copy()
    if "planning_blocks" in context_layers:
        planning = context_layers["planning_blocks"][["bpk", "nama_bpk", "mukim", "geometry"]].rename(
            columns={"mukim": "planning_block_mukim"}
        )
        joined = gpd.sjoin(enriched, planning, how="left", predicate="intersects")
        joined = joined[~joined.index.duplicated(keep="first")]
        enriched["planning_block_join"] = joined["bpk"].reindex(enriched.index)
        enriched["zoning_name_join"] = joined["nama_bpk"].reindex(enriched.index)
        enriched["mukim_from_planning"] = joined["planning_block_mukim"].reindex(enriched.index)
    if "mukim" in context_layers:
        mukim = context_layers["mukim"][["nama", "geometry"]].rename(columns={"nama": "mukim_join"})
        joined = gpd.sjoin(enriched, mukim, how="left", predicate="intersects")
        joined = joined[~joined.index.duplicated(keep="first")]
        enriched["mukim_join"] = joined["mukim_join"].reindex(enriched.index)
    return enriched


def normalize_development_layers(run: PipelineRun, context_layers: dict[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    normalized_frames: list[gpd.GeoDataFrame] = []
    for layer in DEVELOPMENT_LAYERS:
        raw_gdf = _read_geojson_batches(run.raw_root / layer.slug)
        if raw_gdf.empty:
            continue
        raw_gdf.columns = [snake_case(column) for column in raw_gdf.columns]
        if raw_gdf.crs is None:
            raw_gdf = raw_gdf.set_crs("EPSG:4326")
        if raw_gdf.crs.to_epsg() != 4326:
            raw_gdf = raw_gdf.to_crs(4326)
        raw_gdf["geometry"] = _normalize_geometry(raw_gdf.geometry)
        raw_gdf = raw_gdf.loc[raw_gdf.geometry.notnull()].copy()
        raw_gdf = _spatial_enrich(raw_gdf, context_layers)

        raw_attributes = []
        row_hashes = []
        for _, row in raw_gdf.iterrows():
            properties = {k: row[k] for k in raw_gdf.columns if k != "geometry"}
            serializable = {
                key: value.isoformat() if isinstance(value, datetime) else value for key, value in properties.items()
            }
            raw_attributes.append(serializable)
            row_hashes.append(_serialize_record_hash(serializable, row.geometry.wkb_hex))
        raw_gdf["raw_attributes"] = raw_attributes
        raw_gdf["raw_record_hash"] = row_hashes

        for source_field, destination in layer.field_map.items():
            source_key = snake_case(source_field)
            raw_gdf[destination] = raw_gdf.get(source_key)

        for date_field in ("meeting_date_1", "meeting_date_2", "meeting_date_3", "meeting_date_4"):
            raw_gdf[date_field] = _series_or_default(raw_gdf, date_field).map(_arcgis_ms_to_timestamp)

        raw_gdf["application_year"] = _series_or_default(raw_gdf, "application_year").map(_int_or_none)
        raw_gdf["approval_year"] = _series_or_default(raw_gdf, "approval_year").map(_int_or_none)
        raw_gdf["site_area_acres"] = _series_or_default(raw_gdf, "site_area_acres").map(_float_or_none)
        raw_gdf["reference_no"] = _series_or_default(raw_gdf, "reference_no").map(clean_whitespace)
        raw_gdf["reference_no_alt"] = _series_or_default(raw_gdf, "reference_no_alt").map(clean_whitespace)
        raw_gdf["title"] = _series_or_default(raw_gdf, "title").map(clean_whitespace)
        raw_gdf["current_status"] = _series_or_default(raw_gdf, "current_status").map(clean_whitespace)
        raw_gdf["status_raw"] = raw_gdf["current_status"]
        raw_gdf["lot_no"] = _series_or_default(raw_gdf, "lot_no").map(clean_whitespace)
        raw_gdf["mukim"] = _series_or_default(raw_gdf, "mukim").map(normalize_mukim)
        raw_gdf["planning_block"] = _series_or_default(raw_gdf, "planning_block").map(normalize_planning_block)
        raw_gdf["zoning_name"] = _series_or_default(raw_gdf, "zoning_name").map(clean_whitespace)
        raw_gdf["owner_name_raw"] = _series_or_default(raw_gdf, "owner_name_raw").map(clean_whitespace)
        raw_gdf["developer_name"] = _series_or_default(raw_gdf, "developer_name").map(clean_whitespace)
        raw_gdf["consultant_name"] = _series_or_default(raw_gdf, "consultant_name").map(clean_whitespace)
        raw_gdf["proxy_holder_name"] = _series_or_default(raw_gdf, "proxy_holder_name").map(clean_whitespace)
        raw_gdf["mukim"] = (
            raw_gdf["mukim"]
            .fillna(_series_or_default(raw_gdf, "mukim_from_planning"))
            .fillna(_series_or_default(raw_gdf, "mukim_join"))
            .map(normalize_mukim)
        )
        raw_gdf["planning_block"] = (
            raw_gdf["planning_block"]
            .fillna(_series_or_default(raw_gdf, "planning_block_join"))
            .map(normalize_planning_block)
        )
        raw_gdf["zoning_name"] = (
            raw_gdf["zoning_name"].fillna(_series_or_default(raw_gdf, "zoning_name_join")).map(clean_whitespace)
        )

        metric_gdf = raw_gdf.to_crs(3857)
        raw_gdf["area_m2"] = metric_gdf.geometry.area.round(2)
        raw_gdf["area_acres"] = (raw_gdf["area_m2"] / 4046.8564224).round(4)
        raw_gdf["site_area_m2"] = raw_gdf["site_area_acres"].map(
            lambda value: round(value * 4046.8564224, 2) if value is not None else None
        )
        raw_gdf["centroid"] = raw_gdf.geometry.representative_point()
        raw_gdf["application_id"] = raw_gdf["objectid"].map(
            lambda object_id: _stable_application_id(layer.slug, int(object_id))
        )
        raw_gdf["source_system"] = SOURCE_SYSTEM
        raw_gdf["source_municipality"] = MUNICIPALITY
        raw_gdf["source_layer"] = layer.slug
        raw_gdf["source_object_id"] = raw_gdf["objectid"].astype(int)
        raw_gdf["application_type"] = layer.application_type
        raw_gdf["public_display_status"] = raw_gdf["status_raw"].map(normalize_status)
        raw_gdf["public_display_title"] = raw_gdf.apply(
            lambda row: derive_public_title(row["title"], row["owner_name_raw"], row["developer_name"]),
            axis=1,
        )
        raw_gdf["is_public_visible"] = True
        raw_gdf["ingest_run_id"] = run.ingest_run_id
        raw_gdf["created_at"] = run.started_at
        raw_gdf["updated_at"] = run.started_at

        keep_columns = [
            "application_id",
            "ingest_run_id",
            "source_system",
            "source_municipality",
            "source_layer",
            "source_object_id",
            "reference_no",
            "reference_no_alt",
            "title",
            "application_type",
            "current_status",
            "status_raw",
            "application_year",
            "approval_year",
            "meeting_date_1",
            "meeting_decision_1",
            "meeting_date_2",
            "meeting_decision_2",
            "meeting_date_3",
            "meeting_decision_3",
            "meeting_date_4",
            "meeting_decision_4",
            "lot_no",
            "mukim",
            "planning_block",
            "zoning_name",
            "owner_name_raw",
            "developer_name",
            "consultant_name",
            "proxy_holder_name",
            "site_area_acres",
            "site_area_m2",
            "area_m2",
            "area_acres",
            "public_display_title",
            "public_display_status",
            "is_public_visible",
            "raw_record_hash",
            "raw_attributes",
            "centroid",
            "geometry",
            "created_at",
            "updated_at",
        ]
        for column in keep_columns:
            if column not in raw_gdf.columns:
                raw_gdf[column] = None
        stage_gdf = raw_gdf[keep_columns].copy()
        stage_gdf.to_parquet(run.stage_root / f"{layer.slug}.parquet", index=False)
        normalized_frames.append(stage_gdf)

    if not normalized_frames:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    unified = pd.concat(normalized_frames, ignore_index=True)
    gdf = gpd.GeoDataFrame(unified, geometry="geometry", crs="EPSG:4326")
    gdf.to_parquet(run.stage_root / "mbjb_development_unified.parquet", index=False)
    return gdf


def finalize_manifest(run: PipelineRun) -> None:
    write_json(
        run.manifest_path,
        {
            "ingest_run_id": run.ingest_run_id,
            "source_system": SOURCE_SYSTEM,
            "municipality_code": MUNICIPALITY,
            "started_at": run.started_at.isoformat(),
            "observed_counts": run.observed_counts,
            "artifacts": [asdict(artifact) for artifact in run.artifacts],
        },
    )
