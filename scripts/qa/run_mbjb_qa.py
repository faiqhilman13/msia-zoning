from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import psycopg

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.etl.mbjb import EXPECTED_BASELINE_COUNTS


def latest_stage_root() -> Path:
    roots = sorted(CONFIG.data_stage_dir.glob("*"))
    if not roots:
        raise FileNotFoundError("No stage runs found in data/stage/mbjb")
    return roots[-1]


def corresponding_raw_root(stage_root: Path) -> Path:
    return CONFIG.data_raw_dir / stage_root.name


def build_report(stage_root: Path) -> dict:
    gdf = gpd.read_parquet(stage_root / "mbjb_development_unified.parquet")
    boundary = gpd.read_parquet(stage_root / "mbjb_boundary.parquet")
    report: dict[str, object] = {"checks": []}

    counts = gdf.groupby("source_layer").size().to_dict()
    for layer, expected in EXPECTED_BASELINE_COUNTS.items():
        observed = int(counts.get(layer, 0))
        delta_ratio = 0.0 if expected == 0 else (observed - expected) / expected
        report["checks"].append(
            {
                "name": f"baseline_count_{layer}",
                "status": "pass" if observed == expected or delta_ratio > -0.10 else "fail",
                "observed": observed,
                "expected": expected,
                "delta_ratio": round(delta_ratio, 4),
            }
        )

    duplicates = (
        gdf.groupby(["source_system", "source_layer", "source_object_id"]).size().reset_index(name="count")
    )
    duplicate_count = int((duplicates["count"] > 1).sum())
    report["checks"].append(
        {"name": "duplicate_source_ids", "status": "pass" if duplicate_count == 0 else "fail", "observed": duplicate_count}
    )

    invalid_count = int((~gdf.geometry.is_valid).sum())
    zero_area_count = int((gdf["area_m2"] <= 0).sum())
    report["checks"].append(
        {"name": "invalid_geometries", "status": "pass" if invalid_count == 0 else "fail", "observed": invalid_count}
    )
    report["checks"].append(
        {"name": "zero_area_geometries", "status": "pass" if zero_area_count == 0 else "fail", "observed": zero_area_count}
    )

    boundary_union = boundary.geometry.union_all()
    outside_count = int((~gdf.geometry.intersects(boundary_union)).sum())
    report["checks"].append(
        {"name": "outside_mbjb_extent", "status": "pass" if outside_count == 0 else "fail", "observed": outside_count}
    )

    required_fields = ["reference_no", "public_display_title", "public_display_status", "source_layer", "geometry"]
    missing_fields = [field for field in required_fields if field not in gdf.columns]
    report["checks"].append(
        {"name": "required_fields", "status": "pass" if not missing_fields else "fail", "observed": missing_fields}
    )

    return report


def add_db_checks(report: dict, database_url: str) -> dict:
    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        for reference in (
            "MBJB/U/2022/63/STK/KM/15-[15/2022]",
            "MBJB/U/2020/63/STK/KM/4",
            "MBJB/U/2020/63/KJT/EP/1",
        ):
            cur.execute(
                """
                SELECT count(*)
                FROM core.development_applications
                WHERE reference_no = %s
                """,
                (reference,),
            )
            found = cur.fetchone()[0]
            report["checks"].append(
                {
                    "name": f"search_reference_{reference}",
                    "status": "pass" if found >= 1 else "fail",
                    "observed": found,
                }
            )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run QA checks for the latest MBJB stage output.")
    parser.add_argument("--stage-root", help="Specific stage run directory. Defaults to the latest run.")
    parser.add_argument("--database-url", default=CONFIG.database_url)
    args = parser.parse_args()

    stage_root = Path(args.stage_root) if args.stage_root else latest_stage_root()
    raw_root = corresponding_raw_root(stage_root)
    manifest = json.loads((raw_root / "manifest.json").read_text(encoding="utf-8"))
    report = build_report(stage_root)
    report = add_db_checks(report, args.database_url)
    report["ingest_run_id"] = manifest["ingest_run_id"]
    report["stage_root"] = str(stage_root)

    output_path = CONFIG.data_publish_dir / f"qa-{stage_root.name}.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
