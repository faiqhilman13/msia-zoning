from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from malaysia_permits_map.etl.mbpj import PipelineRun
from malaysia_permits_map.etl.mbpj_geometry import normalize_geometry_layers
from malaysia_permits_map.utils.paths import ensure_directory


def _write_geojson(path: Path, features: list[dict]) -> None:
    ensure_directory(path.parent)
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
        encoding="utf-8",
    )


def test_normalize_geometry_layers_creates_context_stage_outputs(tmp_path: Path) -> None:
    run = PipelineRun(
        ingest_run_id="test-run",
        started_at=datetime(2026, 3, 7, tzinfo=UTC),
        raw_root=tmp_path / "raw",
        stage_root=tmp_path / "stage",
        artifacts=[],
        observed_counts={},
    )

    _write_geojson(
        run.raw_root / "official_buildings" / "batch-0001.geojson",
        [
            {
                "type": "Feature",
                "properties": {
                    "OBJECTID": 101,
                    "Kategori": "Bangunan MBPJ",
                    "Nama_Bangu": "Menara MBPJ",
                    "Alamat": "Jalan Yong Shook Lin",
                    "Status_Pem": "Aktif",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [101.6400, 3.1010],
                            [101.6410, 3.1010],
                            [101.6410, 3.1020],
                            [101.6400, 3.1020],
                            [101.6400, 3.1010],
                        ]
                    ],
                },
            }
        ],
    )

    _write_geojson(
        run.raw_root / "municipality_boundary" / "batch-0001.geojson",
        [
            {
                "type": "Feature",
                "properties": {
                    "OBJECTID": 1,
                    "KATEGORI": "PBT",
                    "NAMA_PBT": "Majlis Bandaraya Petaling Jaya",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [101.6000, 3.0700],
                            [101.6600, 3.0700],
                            [101.6600, 3.1300],
                            [101.6000, 3.1300],
                            [101.6000, 3.0700],
                        ]
                    ],
                },
            }
        ],
    )

    outputs = normalize_geometry_layers(run)

    assert set(outputs) == {"official_buildings", "municipality_boundary"}
    assert (run.stage_root / "mbpj_official_buildings.parquet").exists()
    assert (run.stage_root / "mbpj_municipality_boundary.parquet").exists()

    buildings = outputs["official_buildings"]
    assert len(buildings) == 1
    assert buildings.iloc[0]["source_layer"] == "official_buildings"
    assert buildings.iloc[0]["context_type"] == "official_building"
    assert buildings.iloc[0]["name"] == "Menara MBPJ"
    assert buildings.iloc[0]["inspection_status"] == "Aktif"
    assert buildings.iloc[0]["centroid"] is not None

    boundary = outputs["municipality_boundary"]
    assert len(boundary) == 1
    assert boundary.iloc[0]["source_layer"] == "municipality_boundary"
    assert boundary.iloc[0]["context_type"] == "municipality_boundary"
    assert boundary.iloc[0]["name"] == "Majlis Bandaraya Petaling Jaya"
    assert boundary.iloc[0]["geometry"] is not None
