from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.etl.mbpj import EXPECTED_PROJECT_COUNT, SOURCE_LAYER


def latest_stage_root() -> Path:
    roots = sorted(CONFIG.mbpj_data_stage_dir.glob("*"))
    if not roots:
        raise FileNotFoundError("No stage runs found in data/stage/mbpj")
    return roots[-1]


def corresponding_raw_root(stage_root: Path) -> Path:
    return CONFIG.mbpj_data_raw_dir / stage_root.name


def build_report(stage_root: Path) -> dict:
    frame = pd.read_parquet(stage_root / "mbpj_project_register.parquet")
    official_buildings_path = stage_root / "mbpj_official_buildings.parquet"
    boundary_path = stage_root / "mbpj_municipality_boundary.parquet"
    report: dict[str, object] = {"checks": []}

    observed_count = int(len(frame))
    report["checks"].append(
        {
            "name": "baseline_count_approved_project_register",
            "status": "pass" if observed_count == EXPECTED_PROJECT_COUNT else "warn",
            "observed": observed_count,
            "expected": EXPECTED_PROJECT_COUNT,
        }
    )

    duplicate_ids = int(frame["source_object_id"].duplicated().sum())
    duplicate_refs = int(
        frame.loc[frame["reference_no"].notna(), "reference_no"].duplicated().sum()
    )
    missing_public_titles = int(frame["public_display_title"].isna().sum())
    geometry_non_null = int(frame["geometry"].notna().sum()) if "geometry" in frame.columns else 0

    report["checks"].append(
        {"name": "duplicate_source_ids", "status": "pass" if duplicate_ids == 0 else "fail", "observed": duplicate_ids}
    )
    report["checks"].append(
        {
            "name": "duplicate_reference_numbers",
            "status": "pass" if duplicate_refs == 0 else "warn",
            "observed": duplicate_refs,
        }
    )
    report["checks"].append(
        {
            "name": "missing_public_titles",
            "status": "pass" if missing_public_titles == 0 else "fail",
            "observed": missing_public_titles,
        }
    )
    report["checks"].append(
        {
            "name": "text_first_geometry_nullable",
            "status": "pass" if geometry_non_null == 0 else "warn",
            "observed": geometry_non_null,
        }
    )

    official_buildings_count = (
        int(len(pd.read_parquet(official_buildings_path))) if official_buildings_path.exists() else 0
    )
    boundary_count = int(len(pd.read_parquet(boundary_path))) if boundary_path.exists() else 0
    report["checks"].append(
        {
            "name": "context_geometry_official_buildings_present",
            "status": "pass" if official_buildings_count > 0 else "warn",
            "observed": official_buildings_count,
        }
    )
    report["checks"].append(
        {
            "name": "context_geometry_boundary_present",
            "status": "pass" if boundary_count > 0 else "warn",
            "observed": boundary_count,
        }
    )

    required_fields = [
        "reference_no",
        "title",
        "public_display_title",
        "public_display_status",
        "source_layer",
        "source_url",
    ]
    missing_fields = [field for field in required_fields if field not in frame.columns]
    report["checks"].append(
        {"name": "required_fields", "status": "pass" if not missing_fields else "fail", "observed": missing_fields}
    )

    layer_values = sorted(set(frame["source_layer"].dropna().tolist()))
    report["checks"].append(
        {
            "name": "source_layer_contract",
            "status": "pass" if layer_values == [SOURCE_LAYER] else "fail",
            "observed": layer_values,
        }
    )
    return report


def relation_exists(cur: psycopg.Cursor, relation_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation_name,))
    return cur.fetchone()[0] is not None


def add_db_checks(report: dict, database_url: str, ingest_run_id: str) -> dict:
    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        if not relation_exists(cur, "core.development_applications"):
            report["checks"].append(
                {
                    "name": "db_core_development_applications_available",
                    "status": "warn",
                    "observed": "missing relation core.development_applications",
                }
            )
            return report

        cur.execute(
            """
            SELECT count(*)
            FROM core.development_applications
            WHERE source_municipality = 'MBPJ' AND ingest_run_id = %s
            """,
            (ingest_run_id,),
        )
        current_run_count = cur.fetchone()[0]
        report["checks"].append(
            {
                "name": "db_current_run_application_count",
                "status": "pass" if current_run_count == EXPECTED_PROJECT_COUNT else "warn",
                "observed": current_run_count,
                "expected": EXPECTED_PROJECT_COUNT,
            }
        )

        cur.execute(
            """
            SELECT count(*)
            FROM core.development_applications
            WHERE source_municipality = 'MBPJ' AND ingest_run_id <> %s
            """,
            (ingest_run_id,),
        )
        stale_count = cur.fetchone()[0]
        report["checks"].append(
            {
                "name": "db_stale_mbpj_rows",
                "status": "pass" if stale_count == 0 else "fail",
                "observed": stale_count,
            }
        )

        for reference in (
            "MBPJ/040100/T/P23/1/PJS5/0015/2022/SMARTDEV",
            "MBPJ/040100/T/P23/1/PJU1A/0232/2023/SMARTDEV",
        ):
            cur.execute(
                """
                SELECT count(*)
                FROM core.development_applications
                WHERE source_municipality = 'MBPJ' AND ingest_run_id = %s AND reference_no = %s
                """,
                (ingest_run_id, reference),
            )
            found = cur.fetchone()[0]
            report["checks"].append(
                {
                    "name": f"search_reference_{reference}",
                    "status": "pass" if found >= 1 else "fail",
                    "observed": found,
                }
            )

        if relation_exists(cur, "core.context_features"):
            cur.execute(
                """
                SELECT count(*)
                FROM core.context_features
                WHERE municipality_code = 'MBPJ' AND context_type = 'official_building'
                """
            )
            context_count = cur.fetchone()[0]
            report["checks"].append(
                {
                    "name": "db_context_features_official_building_count",
                    "status": "pass" if context_count > 0 else "warn",
                    "observed": context_count,
                }
            )
        else:
            report["checks"].append(
                {
                    "name": "db_context_features_official_building_count",
                    "status": "warn",
                    "observed": "missing relation core.context_features",
                }
            )

        if relation_exists(cur, "core.admin_boundaries"):
            cur.execute(
                """
                SELECT count(*)
                FROM core.admin_boundaries
                WHERE municipality_code = 'MBPJ' AND boundary_type = 'municipality'
                """
            )
            boundary_count = cur.fetchone()[0]
            report["checks"].append(
                {
                    "name": "db_context_boundary_count",
                    "status": "pass" if boundary_count > 0 else "warn",
                    "observed": boundary_count,
                }
            )
        else:
            report["checks"].append(
                {
                    "name": "db_context_boundary_count",
                    "status": "warn",
                    "observed": "missing relation core.admin_boundaries",
                }
            )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run QA checks for the latest MBPJ stage output.")
    parser.add_argument("--stage-root", help="Specific stage run directory. Defaults to the latest run.")
    parser.add_argument("--database-url", default=CONFIG.database_url)
    args = parser.parse_args()

    stage_root = Path(args.stage_root) if args.stage_root else latest_stage_root()
    raw_root = corresponding_raw_root(stage_root)
    manifest = json.loads((raw_root / "manifest.json").read_text(encoding="utf-8"))
    report = build_report(stage_root)
    report = add_db_checks(report, args.database_url, manifest["ingest_run_id"])
    report["ingest_run_id"] = manifest["ingest_run_id"]
    report["stage_root"] = str(stage_root)

    output_path = CONFIG.data_publish_dir / f"qa-mbpj-{stage_root.name}.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
