from __future__ import annotations

import hashlib
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd

from malaysia_permits_map.config import AppConfig
from malaysia_permits_map.etl.arcgis_client import ArcGISClient, write_json
from malaysia_permits_map.etl.mbjb import _normalize_geometry, _read_geojson_batches, chunked
from malaysia_permits_map.etl.mbpj import PipelineRun
from malaysia_permits_map.models.arcgis import ArcGISLayerConfig
from malaysia_permits_map.utils.paths import ensure_directory
from malaysia_permits_map.utils.text import clean_whitespace, snake_case


SOURCE_SYSTEM = "mbpj_arcgis"

GEOMETRY_LAYERS: tuple[ArcGISLayerConfig, ...] = (
    ArcGISLayerConfig(
        slug="official_buildings",
        label="MBPJ Official Buildings",
        service_name="MBPJ",
        layer_id=2,
        application_type="Official Building",
        kind="context",
        field_map={
            "Kategori": "category",
            "Nama_Bangu": "name",
            "Alamat": "address",
            "Status_Pem": "inspection_status",
        },
    ),
    ArcGISLayerConfig(
        slug="municipality_boundary",
        label="MBPJ Boundary",
        service_name="MBPJ",
        layer_id=3,
        application_type="Municipality Boundary",
        kind="context",
        field_map={
            "LAYER": "layer_name",
            "KATEGORI": "category",
            "NAMA_PBT": "authority_name",
            "BANDAR": "city_name",
            "LUAS_HEK": "area_hectares",
        },
    ),
)


def ingest_geometry_layers(run: PipelineRun, config: AppConfig) -> None:
    client = ArcGISClient(
        config,
        service_root=config.mbpj_arcgis_service_root,
        timeout_seconds=config.mbpj_request_timeout_seconds,
        retry_attempts=config.mbpj_retry_attempts,
    )
    try:
        for layer in GEOMETRY_LAYERS:
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
    finally:
        client.close()


def _serialize_hash(properties: dict[str, object], geometry_wkb_hex: str) -> str:
    payload = json.dumps(properties, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload + geometry_wkb_hex.encode("utf-8")).hexdigest()


def normalize_geometry_layers(run: PipelineRun) -> dict[str, gpd.GeoDataFrame]:
    ensure_directory(run.stage_root)
    outputs: dict[str, gpd.GeoDataFrame] = {}
    for layer in GEOMETRY_LAYERS:
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

        for source_field, destination in layer.field_map.items():
            gdf[destination] = gdf.get(snake_case(source_field))

        raw_attributes = []
        row_hashes = []
        for _, row in gdf.iterrows():
            properties = {key: row[key] for key in gdf.columns if key != "geometry"}
            serializable = {key: value for key, value in properties.items()}
            raw_attributes.append(serializable)
            row_hashes.append(_serialize_hash(serializable, row.geometry.wkb_hex))

        gdf["raw_attributes"] = raw_attributes
        gdf["raw_record_hash"] = row_hashes
        gdf["source_system"] = SOURCE_SYSTEM
        gdf["source_layer"] = layer.slug
        gdf["source_object_id"] = gdf["objectid"].astype(int)
        gdf["municipality_code"] = "MBPJ"
        gdf["context_type"] = "official_building" if layer.slug == "official_buildings" else "municipality_boundary"
        gdf["name"] = gdf.get("name", gdf.get("authority_name")).map(clean_whitespace)
        gdf["category"] = gdf.get("category").map(clean_whitespace)
        gdf["address"] = gdf.get("address").map(clean_whitespace) if "address" in gdf.columns else None
        gdf["inspection_status"] = (
            gdf.get("inspection_status").map(clean_whitespace) if "inspection_status" in gdf.columns else None
        )
        gdf["centroid"] = gdf.geometry.representative_point()

        keep_columns = [
            "municipality_code",
            "context_type",
            "source_system",
            "source_layer",
            "source_object_id",
            "name",
            "category",
            "address",
            "inspection_status",
            "raw_record_hash",
            "raw_attributes",
            "centroid",
            "geometry",
        ]
        for column in keep_columns:
            if column not in gdf.columns:
                gdf[column] = None

        stage_gdf = gdf[keep_columns].copy()
        stage_gdf.to_parquet(run.stage_root / f"mbpj_{layer.slug}.parquet", index=False)
        outputs[layer.slug] = stage_gdf
    return outputs
