from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.db.sql import apply_sql_file
from malaysia_permits_map.etl.mbpj import SOURCE_SYSTEM


GEOMETRY_STAGE_FILES = {
    "official_buildings": "mbpj_official_buildings.parquet",
    "municipality_boundary": "mbpj_municipality_boundary.parquet",
}


def latest_stage_root() -> Path:
    roots = sorted(CONFIG.mbpj_data_stage_dir.glob("*"))
    if not roots:
        raise FileNotFoundError("No stage runs found in data/stage/mbpj")
    return roots[-1]


def corresponding_raw_root(stage_root: Path) -> Path:
    raw_root = CONFIG.mbpj_data_raw_dir / stage_root.name
    if not raw_root.exists():
        raise FileNotFoundError(f"Missing raw root for stage run: {raw_root}")
    return raw_root


def load_stage_df(stage_root: Path) -> pd.DataFrame:
    return pd.read_parquet(stage_root / "mbpj_project_register.parquet")


def load_context_gdf(stage_root: Path, layer_slug: str) -> gpd.GeoDataFrame:
    path = stage_root / GEOMETRY_STAGE_FILES[layer_slug]
    if not path.exists():
        raise FileNotFoundError(f"Missing MBPJ context stage file: {path}")
    frame = gpd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"MBPJ context stage file is empty: {path}")
    return frame


def normalize_artifact_relative_path(raw_root: Path, value: str) -> str:
    path_text = str(value).replace("\\", "/").strip()
    if not path_text:
        return path_text

    candidate = Path(path_text)
    if not candidate.is_absolute():
        return path_text.lstrip("./")

    try:
        return candidate.relative_to(raw_root).as_posix()
    except ValueError:
        marker = f"/{raw_root.name}/"
        if marker in path_text:
            return path_text.split(marker, 1)[1].lstrip("/")
        return candidate.name


def to_native(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, dict, list)):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def apply_migrations(conn: psycopg.Connection) -> None:
    for migration_path in sorted(CONFIG.migrations_dir.glob("*.sql")):
        apply_sql_file(conn, migration_path)


def insert_meta(conn: psycopg.Connection, raw_root: Path, stage_root: Path, status: str) -> tuple[str, dict]:
    manifest = json.loads((raw_root / "manifest.json").read_text(encoding="utf-8"))
    ingest_run_id = manifest["ingest_run_id"]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.ingest_runs (
                ingest_run_id, source_system, municipality_code, started_at, run_status,
                run_label, observed_counts, raw_root, stage_root
            ) VALUES (
                %(ingest_run_id)s, %(source_system)s, %(municipality_code)s, %(started_at)s, %(run_status)s,
                %(run_label)s, %(observed_counts)s::jsonb, %(raw_root)s, %(stage_root)s
            )
            ON CONFLICT (ingest_run_id) DO UPDATE
            SET run_status = EXCLUDED.run_status,
                observed_counts = EXCLUDED.observed_counts,
                raw_root = EXCLUDED.raw_root,
                stage_root = EXCLUDED.stage_root
            """,
            {
                "ingest_run_id": ingest_run_id,
                "source_system": manifest["source_system"],
                "municipality_code": manifest["municipality_code"],
                "started_at": manifest["started_at"],
                "run_status": status,
                "run_label": stage_root.name,
                "observed_counts": json.dumps(manifest["observed_counts"]),
                "raw_root": str(raw_root),
                "stage_root": str(stage_root),
            },
        )
        cur.execute("DELETE FROM meta.ingest_artifacts WHERE ingest_run_id = %s", (ingest_run_id,))
        for artifact in manifest.get("artifacts", []):
            cur.execute(
                """
                INSERT INTO meta.ingest_artifacts (
                    ingest_run_id, artifact_type, relative_path, file_size_bytes, sha256
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    ingest_run_id,
                    artifact["artifact_type"],
                    normalize_artifact_relative_path(raw_root, artifact["relative_path"]),
                    artifact["file_size_bytes"],
                    artifact["sha256"],
                ),
            )
    conn.commit()
    return ingest_run_id, manifest


def load_raw_table(conn: psycopg.Connection, frame: pd.DataFrame, ingest_run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.mbpj_project_register WHERE ingest_run_id = %s", (ingest_run_id,))
        for _, row in frame.iterrows():
            cur.execute(
                """
                INSERT INTO raw.mbpj_project_register (
                    ingest_run_id, source_object_id, source_row_number, reference_no,
                    raw_attributes, raw_record_hash
                ) VALUES (
                    %(ingest_run_id)s, %(source_object_id)s, %(source_row_number)s, %(reference_no)s,
                    %(raw_attributes)s::jsonb, %(raw_record_hash)s
                )
                """,
                {
                    "ingest_run_id": ingest_run_id,
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "source_row_number": int(to_native(row["source_row_number"])),
                    "reference_no": to_native(row["reference_no"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                },
            )
    conn.commit()


def load_stage_and_core(conn: psycopg.Connection, frame: pd.DataFrame, ingest_run_id: str) -> None:
    application_ids = [str(application_id) for application_id in frame["application_id"].tolist()]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM stage.mbpj_project_register")
        if application_ids:
            cur.execute(
                """
                DELETE FROM core.development_applications
                WHERE source_municipality = 'MBPJ'
                  AND source_system = %s
                  AND application_id <> ALL(%s::uuid[])
                """,
                (SOURCE_SYSTEM, application_ids),
            )
        else:
            cur.execute(
                """
                DELETE FROM core.development_applications
                WHERE source_municipality = 'MBPJ'
                  AND source_system = %s
                """,
                (SOURCE_SYSTEM,),
            )

        for _, row in frame.iterrows():
            payload = {
                "application_id": row["application_id"],
                "ingest_run_id": ingest_run_id,
                "source_system": row["source_system"],
                "source_municipality": row["source_municipality"],
                "source_layer": row["source_layer"],
                "source_object_id": int(to_native(row["source_object_id"])),
                "reference_no": to_native(row["reference_no"]),
                "reference_no_alt": to_native(row["reference_no_alt"]),
                "title": to_native(row["title"]),
                "application_type": to_native(row["application_type"]),
                "current_status": to_native(row["current_status"]),
                "status_raw": to_native(row["status_raw"]),
                "application_year": to_native(row["application_year"]),
                "approval_year": to_native(row["approval_year"]),
                "meeting_date_1": to_native(row["meeting_date_1"]),
                "meeting_decision_1": to_native(row["meeting_decision_1"]),
                "meeting_date_2": to_native(row["meeting_date_2"]),
                "meeting_decision_2": to_native(row["meeting_decision_2"]),
                "meeting_date_3": to_native(row["meeting_date_3"]),
                "meeting_decision_3": to_native(row["meeting_decision_3"]),
                "meeting_date_4": to_native(row["meeting_date_4"]),
                "meeting_decision_4": to_native(row["meeting_decision_4"]),
                "lot_no": to_native(row["lot_no"]),
                "mukim": to_native(row["mukim"]),
                "planning_block": to_native(row["planning_block"]),
                "zoning_name": to_native(row["zoning_name"]),
                "owner_name_raw": to_native(row["owner_name_raw"]),
                "developer_name": to_native(row["developer_name"]),
                "consultant_name": to_native(row["consultant_name"]),
                "proxy_holder_name": to_native(row["proxy_holder_name"]),
                "site_area_acres": to_native(row["site_area_acres"]),
                "site_area_m2": to_native(row["site_area_m2"]),
                "area_m2": to_native(row["area_m2"]),
                "area_acres": to_native(row["area_acres"]),
                "public_display_title": to_native(row["public_display_title"]),
                "public_display_status": to_native(row["public_display_status"]),
                "is_public_visible": bool(to_native(row["is_public_visible"])),
                "raw_record_hash": to_native(row["raw_record_hash"]),
                "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                "source_row_number": int(to_native(row["source_row_number"])),
                "source_url": to_native(row["source_url"]),
                "created_at": to_native(row["created_at"]),
                "updated_at": to_native(row["updated_at"]),
            }
            cur.execute(
                """
                INSERT INTO stage.mbpj_project_register (
                    application_id, ingest_run_id, source_system, source_municipality, source_layer,
                    source_object_id, source_row_number, source_url, reference_no, reference_no_alt,
                    title, application_type, current_status, status_raw, application_year, approval_year,
                    meeting_date_1, meeting_decision_1, meeting_date_2, meeting_decision_2,
                    meeting_date_3, meeting_decision_3, meeting_date_4, meeting_decision_4,
                    lot_no, mukim, planning_block, zoning_name, owner_name_raw,
                    developer_name, consultant_name, proxy_holder_name, site_area_acres, site_area_m2,
                    area_m2, area_acres, public_display_title, public_display_status, is_public_visible,
                    raw_record_hash, raw_attributes, created_at, updated_at
                ) VALUES (
                    %(application_id)s, %(ingest_run_id)s, %(source_system)s, %(source_municipality)s, %(source_layer)s,
                    %(source_object_id)s, %(source_row_number)s, %(source_url)s, %(reference_no)s, %(reference_no_alt)s,
                    %(title)s, %(application_type)s, %(current_status)s, %(status_raw)s, %(application_year)s, %(approval_year)s,
                    %(meeting_date_1)s, %(meeting_decision_1)s, %(meeting_date_2)s, %(meeting_decision_2)s,
                    %(meeting_date_3)s, %(meeting_decision_3)s, %(meeting_date_4)s, %(meeting_decision_4)s,
                    %(lot_no)s, %(mukim)s, %(planning_block)s, %(zoning_name)s, %(owner_name_raw)s,
                    %(developer_name)s, %(consultant_name)s, %(proxy_holder_name)s, %(site_area_acres)s, %(site_area_m2)s,
                    %(area_m2)s, %(area_acres)s, %(public_display_title)s, %(public_display_status)s, %(is_public_visible)s,
                    %(raw_record_hash)s, %(raw_attributes)s::jsonb, %(created_at)s, %(updated_at)s
                )
                """,
                payload,
            )
            cur.execute(
                """
                INSERT INTO core.development_applications (
                    application_id, source_system, source_municipality, source_layer, source_object_id,
                    reference_no, reference_no_alt, title, application_type, current_status, status_raw,
                    application_year, approval_year, meeting_date_1, meeting_decision_1, meeting_date_2,
                    meeting_decision_2, meeting_date_3, meeting_decision_3, meeting_date_4, meeting_decision_4,
                    lot_no, mukim, planning_block, zoning_name, owner_name_raw, developer_name,
                    consultant_name, proxy_holder_name, site_area_acres, site_area_m2,
                    public_display_title, public_display_status, is_public_visible, raw_record_hash,
                    ingest_run_id, created_at, updated_at
                ) VALUES (
                    %(application_id)s, %(source_system)s, %(source_municipality)s, %(source_layer)s, %(source_object_id)s,
                    %(reference_no)s, %(reference_no_alt)s, %(title)s, %(application_type)s, %(current_status)s, %(status_raw)s,
                    %(application_year)s, %(approval_year)s, %(meeting_date_1)s, %(meeting_decision_1)s, %(meeting_date_2)s,
                    %(meeting_decision_2)s, %(meeting_date_3)s, %(meeting_decision_3)s, %(meeting_date_4)s, %(meeting_decision_4)s,
                    %(lot_no)s, %(mukim)s, %(planning_block)s, %(zoning_name)s, %(owner_name_raw)s, %(developer_name)s,
                    %(consultant_name)s, %(proxy_holder_name)s, %(site_area_acres)s, %(site_area_m2)s,
                    %(public_display_title)s, %(public_display_status)s, %(is_public_visible)s, %(raw_record_hash)s,
                    %(ingest_run_id)s, %(created_at)s, %(updated_at)s
                )
                ON CONFLICT (application_id) DO UPDATE
                SET
                    reference_no = EXCLUDED.reference_no,
                    reference_no_alt = EXCLUDED.reference_no_alt,
                    title = EXCLUDED.title,
                    application_type = EXCLUDED.application_type,
                    current_status = EXCLUDED.current_status,
                    status_raw = EXCLUDED.status_raw,
                    application_year = EXCLUDED.application_year,
                    approval_year = EXCLUDED.approval_year,
                    lot_no = EXCLUDED.lot_no,
                    mukim = EXCLUDED.mukim,
                    planning_block = EXCLUDED.planning_block,
                    zoning_name = EXCLUDED.zoning_name,
                    owner_name_raw = EXCLUDED.owner_name_raw,
                    developer_name = EXCLUDED.developer_name,
                    consultant_name = EXCLUDED.consultant_name,
                    proxy_holder_name = EXCLUDED.proxy_holder_name,
                    site_area_acres = EXCLUDED.site_area_acres,
                    site_area_m2 = EXCLUDED.site_area_m2,
                    public_display_title = EXCLUDED.public_display_title,
                    public_display_status = EXCLUDED.public_display_status,
                    raw_record_hash = EXCLUDED.raw_record_hash,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    updated_at = EXCLUDED.updated_at
                """,
                payload,
            )

        cur.execute(
            """
            INSERT INTO core.search_documents (
                application_id, reference_no, title, developer_name, consultant_name, lot_no, mukim,
                planning_block, search_tsv
            )
            SELECT
                application_id,
                reference_no,
                public_display_title,
                developer_name,
                consultant_name,
                lot_no,
                mukim,
                planning_block,
                to_tsvector(
                    'simple',
                    concat_ws(
                        ' ',
                        coalesce(reference_no, ''),
                        coalesce(reference_no_alt, ''),
                        coalesce(public_display_title, ''),
                        coalesce(developer_name, ''),
                        coalesce(consultant_name, ''),
                        coalesce(lot_no, ''),
                        coalesce(mukim, ''),
                        coalesce(planning_block, '')
                    )
                )
            FROM core.development_applications
            WHERE ingest_run_id = %s
            ON CONFLICT (application_id) DO UPDATE
            SET
                reference_no = EXCLUDED.reference_no,
                title = EXCLUDED.title,
                developer_name = EXCLUDED.developer_name,
                consultant_name = EXCLUDED.consultant_name,
                lot_no = EXCLUDED.lot_no,
                mukim = EXCLUDED.mukim,
                planning_block = EXCLUDED.planning_block,
                search_tsv = EXCLUDED.search_tsv,
                updated_at = now()
            """,
            (ingest_run_id,),
        )
    conn.commit()


def load_context(conn: psycopg.Connection, stage_root: Path, ingest_run_id: str) -> None:
    official_buildings = load_context_gdf(stage_root, "official_buildings")
    boundary = load_context_gdf(stage_root, "municipality_boundary")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.mbpj_official_buildings WHERE ingest_run_id = %s", (ingest_run_id,))
        for _, row in official_buildings.iterrows():
            cur.execute(
                """
                INSERT INTO raw.mbpj_official_buildings (
                    ingest_run_id, source_object_id, source_layer, name,
                    raw_attributes, raw_record_hash, geometry
                ) VALUES (
                    %(ingest_run_id)s, %(source_object_id)s, %(source_layer)s, %(name)s,
                    %(raw_attributes)s::jsonb, %(raw_record_hash)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "ingest_run_id": ingest_run_id,
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "source_layer": to_native(row["source_layer"]),
                    "name": to_native(row["name"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute("DELETE FROM raw.mbpj_boundary WHERE ingest_run_id = %s", (ingest_run_id,))
        for _, row in boundary.iterrows():
            cur.execute(
                """
                INSERT INTO raw.mbpj_boundary (
                    ingest_run_id, source_object_id, source_layer, name,
                    raw_attributes, raw_record_hash, geometry
                ) VALUES (
                    %(ingest_run_id)s, %(source_object_id)s, %(source_layer)s, %(name)s,
                    %(raw_attributes)s::jsonb, %(raw_record_hash)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "ingest_run_id": ingest_run_id,
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "source_layer": to_native(row["source_layer"]),
                    "name": to_native(row["name"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute("DELETE FROM stage.mbpj_official_buildings")
        for _, row in official_buildings.iterrows():
            cur.execute(
                """
                INSERT INTO stage.mbpj_official_buildings (
                    source_object_id, municipality_code, context_type, source_system, source_layer,
                    name, category, address, inspection_status, raw_record_hash, raw_attributes,
                    centroid, geometry
                ) VALUES (
                    %(source_object_id)s, %(municipality_code)s, %(context_type)s, %(source_system)s, %(source_layer)s,
                    %(name)s, %(category)s, %(address)s, %(inspection_status)s, %(raw_record_hash)s, %(raw_attributes)s::jsonb,
                    ST_GeomFromText(%(centroid_wkt)s, 4326),
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "municipality_code": to_native(row["municipality_code"]),
                    "context_type": to_native(row["context_type"]),
                    "source_system": to_native(row["source_system"]),
                    "source_layer": to_native(row["source_layer"]),
                    "name": to_native(row["name"]),
                    "category": to_native(row["category"]),
                    "address": to_native(row["address"]),
                    "inspection_status": to_native(row["inspection_status"]),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "centroid_wkt": row["centroid"].wkt,
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute("DELETE FROM stage.mbpj_boundary")
        for _, row in boundary.iterrows():
            cur.execute(
                """
                INSERT INTO stage.mbpj_boundary (
                    source_object_id, municipality_code, context_type, source_system, source_layer,
                    name, category, raw_record_hash, raw_attributes, centroid, geometry
                ) VALUES (
                    %(source_object_id)s, %(municipality_code)s, %(context_type)s, %(source_system)s, %(source_layer)s,
                    %(name)s, %(category)s, %(raw_record_hash)s, %(raw_attributes)s::jsonb,
                    ST_GeomFromText(%(centroid_wkt)s, 4326),
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "municipality_code": to_native(row["municipality_code"]),
                    "context_type": to_native(row["context_type"]),
                    "source_system": to_native(row["source_system"]),
                    "source_layer": to_native(row["source_layer"]),
                    "name": to_native(row["name"]),
                    "category": to_native(row["category"]),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "centroid_wkt": row["centroid"].wkt,
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute(
            "DELETE FROM core.context_features WHERE municipality_code = 'MBPJ' AND context_type = 'official_building'"
        )
        for _, row in official_buildings.iterrows():
            cur.execute(
                """
                INSERT INTO core.context_features (
                    municipality_code, context_type, name, category, address, inspection_status,
                    source_system, source_layer, source_object_id, raw_record_hash, raw_attributes,
                    centroid, geometry
                ) VALUES (
                    %(municipality_code)s, %(context_type)s, %(name)s, %(category)s, %(address)s, %(inspection_status)s,
                    %(source_system)s, %(source_layer)s, %(source_object_id)s, %(raw_record_hash)s, %(raw_attributes)s::jsonb,
                    ST_GeomFromText(%(centroid_wkt)s, 4326),
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "municipality_code": to_native(row["municipality_code"]),
                    "context_type": to_native(row["context_type"]),
                    "name": to_native(row["name"]),
                    "category": to_native(row["category"]),
                    "address": to_native(row["address"]),
                    "inspection_status": to_native(row["inspection_status"]),
                    "source_system": to_native(row["source_system"]),
                    "source_layer": to_native(row["source_layer"]),
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "raw_record_hash": to_native(row["raw_record_hash"]),
                    "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                    "centroid_wkt": row["centroid"].wkt,
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute("DELETE FROM core.admin_boundaries WHERE municipality_code = 'MBPJ'")
        for _, row in boundary.iterrows():
            cur.execute(
                """
                INSERT INTO core.admin_boundaries (
                    municipality_code, boundary_type, boundary_code, boundary_name,
                    source_system, source_layer, source_object_id, geometry
                ) VALUES (
                    %(municipality_code)s, 'municipality', %(boundary_code)s, %(boundary_name)s,
                    %(source_system)s, %(source_layer)s, %(source_object_id)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "municipality_code": to_native(row["municipality_code"]),
                    "boundary_code": "MBPJ",
                    "boundary_name": to_native(row["name"]) or "Majlis Bandaraya Petaling Jaya",
                    "source_system": to_native(row["source_system"]),
                    "source_layer": to_native(row["source_layer"]),
                    "source_object_id": int(to_native(row["source_object_id"])),
                    "geometry_wkt": row.geometry.wkt,
                },
            )
    conn.commit()


def mark_run(conn: psycopg.Connection, ingest_run_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.ingest_runs
            SET run_status = %s, completed_at = now()
            WHERE ingest_run_id = %s
            """,
            (status, ingest_run_id),
        )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the latest MBPJ stage output into PostGIS.")
    parser.add_argument("--stage-root", help="Specific stage run directory. Defaults to the latest run.")
    parser.add_argument(
        "--skip-context",
        action="store_true",
        help="Skip refreshing MBPJ context geometry. Use this only for older text-only runs.",
    )
    args = parser.parse_args()

    stage_root = Path(args.stage_root) if args.stage_root else latest_stage_root()
    raw_root = corresponding_raw_root(stage_root)
    frame = load_stage_df(stage_root)

    with psycopg.connect(CONFIG.database_url) as conn:
        apply_migrations(conn)
        ingest_run_id, _manifest = insert_meta(conn, raw_root, stage_root, status="publishing")
        load_raw_table(conn, frame, ingest_run_id)
        load_stage_and_core(conn, frame, ingest_run_id)
        if not args.skip_context:
            load_context(conn, stage_root, ingest_run_id)
        mark_run(conn, ingest_run_id, "published")

    print(f"Loaded MBPJ run {stage_root.name} into PostGIS")


if __name__ == "__main__":
    main()
