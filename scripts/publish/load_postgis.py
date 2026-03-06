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


def latest_stage_root() -> Path:
    roots = sorted(CONFIG.data_stage_dir.glob("*"))
    if not roots:
        raise FileNotFoundError("No stage runs found in data/stage/mbjb")
    return roots[-1]


def corresponding_raw_root(stage_root: Path) -> Path:
    raw_root = CONFIG.data_raw_dir / stage_root.name
    if not raw_root.exists():
        raise FileNotFoundError(f"Missing raw root for stage run: {raw_root}")
    return raw_root


def load_stage_gdf(stage_root: Path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(stage_root / "mbjb_development_unified.parquet")


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
                    artifact["relative_path"],
                    artifact["file_size_bytes"],
                    artifact["sha256"],
                ),
            )
    conn.commit()
    return ingest_run_id, manifest


def apply_migrations(conn: psycopg.Connection) -> None:
    for migration_path in sorted(CONFIG.migrations_dir.glob("*.sql")):
        apply_sql_file(conn, migration_path)


def load_raw_tables(conn: psycopg.Connection, gdf: gpd.GeoDataFrame, ingest_run_id: str) -> None:
    table_map = {
        "kebenaran_merancang": "raw.mbjb_kebenaran_merancang",
        "pelan_bangunan": "raw.mbjb_pelan_bangunan",
        "kerja_tanah": "raw.mbjb_kerja_tanah",
    }
    with conn.cursor() as cur:
        for layer_slug, table_name in table_map.items():
            cur.execute(f"DELETE FROM {table_name} WHERE ingest_run_id = %s", (ingest_run_id,))
            subset = gdf.loc[gdf["source_layer"] == layer_slug]
            for _, row in subset.iterrows():
                cur.execute(
                    f"""
                    INSERT INTO {table_name} (
                        ingest_run_id, source_object_id, source_layer, reference_no,
                        raw_attributes, raw_record_hash, geometry
                    ) VALUES (
                        %(ingest_run_id)s, %(source_object_id)s, %(source_layer)s, %(reference_no)s,
                        %(raw_attributes)s::jsonb, %(raw_record_hash)s,
                        ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                    )
                    """,
                    {
                        "ingest_run_id": ingest_run_id,
                        "source_object_id": int(row["source_object_id"]),
                        "source_layer": row["source_layer"],
                        "reference_no": row["reference_no"],
                        "raw_attributes": json.dumps(row["raw_attributes"], default=str),
                        "raw_record_hash": row["raw_record_hash"],
                        "geometry_wkt": row.geometry.wkt,
                    },
                )
    conn.commit()


def load_stage_and_core(conn: psycopg.Connection, gdf: gpd.GeoDataFrame, ingest_run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stage.mbjb_development_unified WHERE ingest_run_id = %s", (ingest_run_id,))
        application_ids = [str(application_id) for application_id in gdf["application_id"].tolist()]
        if application_ids:
            cur.execute(
                "DELETE FROM stage.mbjb_development_unified WHERE application_id = ANY(%s)",
                (application_ids,),
            )
        for _, row in gdf.iterrows():
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
                "centroid_wkt": row["centroid"].wkt,
                "geometry_wkt": row.geometry.wkt,
                "created_at": to_native(row["created_at"]),
                "updated_at": to_native(row["updated_at"]),
            }
            cur.execute(
                """
                INSERT INTO stage.mbjb_development_unified (
                    application_id, ingest_run_id, source_system, source_municipality, source_layer,
                    source_object_id, reference_no, reference_no_alt, title, application_type,
                    current_status, status_raw, application_year, approval_year,
                    meeting_date_1, meeting_decision_1, meeting_date_2, meeting_decision_2,
                    meeting_date_3, meeting_decision_3, meeting_date_4, meeting_decision_4,
                    lot_no, mukim, planning_block, zoning_name, owner_name_raw,
                    developer_name, consultant_name, proxy_holder_name, site_area_acres, site_area_m2,
                    area_m2, area_acres, public_display_title, public_display_status, is_public_visible,
                    raw_record_hash, raw_attributes, centroid, geometry, created_at, updated_at
                ) VALUES (
                    %(application_id)s, %(ingest_run_id)s, %(source_system)s, %(source_municipality)s, %(source_layer)s,
                    %(source_object_id)s, %(reference_no)s, %(reference_no_alt)s, %(title)s, %(application_type)s,
                    %(current_status)s, %(status_raw)s, %(application_year)s, %(approval_year)s,
                    %(meeting_date_1)s, %(meeting_decision_1)s, %(meeting_date_2)s, %(meeting_decision_2)s,
                    %(meeting_date_3)s, %(meeting_decision_3)s, %(meeting_date_4)s, %(meeting_decision_4)s,
                    %(lot_no)s, %(mukim)s, %(planning_block)s, %(zoning_name)s, %(owner_name_raw)s,
                    %(developer_name)s, %(consultant_name)s, %(proxy_holder_name)s, %(site_area_acres)s, %(site_area_m2)s,
                    %(area_m2)s, %(area_acres)s, %(public_display_title)s, %(public_display_status)s, %(is_public_visible)s,
                    %(raw_record_hash)s, %(raw_attributes)s::jsonb,
                    ST_GeomFromText(%(centroid_wkt)s, 4326),
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326)),
                    %(created_at)s, %(updated_at)s
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
                INSERT INTO core.development_geometries (
                    application_id, geometry, centroid, area_m2, area_acres, is_valid_geometry
                ) VALUES (
                    %(application_id)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326)),
                    ST_GeomFromText(%(centroid_wkt)s, 4326),
                    %(area_m2)s,
                    %(area_acres)s,
                    true
                )
                ON CONFLICT (application_id) DO UPDATE
                SET
                    geometry = EXCLUDED.geometry,
                    centroid = EXCLUDED.centroid,
                    area_m2 = EXCLUDED.area_m2,
                    area_acres = EXCLUDED.area_acres,
                    is_valid_geometry = EXCLUDED.is_valid_geometry
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


def load_context(conn: psycopg.Connection, stage_root: Path) -> None:
    planning_blocks = gpd.read_parquet(stage_root / "planning_blocks.parquet")
    mukim = gpd.read_parquet(stage_root / "mukim.parquet")
    boundary = gpd.read_parquet(stage_root / "mbjb_boundary.parquet")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM core.zoning_areas WHERE municipality_code = 'MBJB' AND zoning_type = 'planning_block'")
        for _, row in planning_blocks.iterrows():
            cur.execute(
                """
                INSERT INTO core.zoning_areas (
                    municipality_code, zoning_type, zoning_code, zoning_name, mukim,
                    source_system, source_layer, source_object_id, geometry
                ) VALUES (
                    'MBJB', 'planning_block', %(zoning_code)s, %(zoning_name)s, %(mukim)s,
                    'mbjb_geojb', 'planning_blocks', %(source_object_id)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "zoning_code": row.get("bpk"),
                    "zoning_name": row.get("nama_bpk"),
                    "mukim": row.get("mukim"),
                    "source_object_id": int(row.get("objectid")),
                    "geometry_wkt": row.geometry.wkt,
                },
            )

        cur.execute("DELETE FROM core.admin_boundaries WHERE municipality_code = 'MBJB'")
        for _, row in mukim.iterrows():
            cur.execute(
                """
                INSERT INTO core.admin_boundaries (
                    municipality_code, boundary_type, boundary_code, boundary_name,
                    source_system, source_layer, source_object_id, geometry
                ) VALUES (
                    'MBJB', 'mukim', %(boundary_code)s, %(boundary_name)s,
                    'mbjb_geojb', 'mukim', %(source_object_id)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "boundary_code": row.get("kod_mukim"),
                    "boundary_name": row.get("nama"),
                    "source_object_id": int(row.get("objectid")),
                    "geometry_wkt": row.geometry.wkt,
                },
            )
        for _, row in boundary.iterrows():
            cur.execute(
                """
                INSERT INTO core.admin_boundaries (
                    municipality_code, boundary_type, boundary_code, boundary_name,
                    source_system, source_layer, source_object_id, geometry
                ) VALUES (
                    'MBJB', 'municipality', %(boundary_code)s, %(boundary_name)s,
                    'mbjb_geojb', 'mbjb_boundary', %(source_object_id)s,
                    ST_Multi(ST_GeomFromText(%(geometry_wkt)s, 4326))
                )
                """,
                {
                    "boundary_code": row.get("kod") or "MBJB",
                    "boundary_name": row.get("nama") or "Majlis Bandaraya Johor Bahru",
                    "source_object_id": int(row.get("objectid")),
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
    parser = argparse.ArgumentParser(description="Load the latest MBJB stage output into PostGIS.")
    parser.add_argument("--stage-root", help="Specific stage run directory. Defaults to the latest run.")
    args = parser.parse_args()

    stage_root = Path(args.stage_root) if args.stage_root else latest_stage_root()
    raw_root = corresponding_raw_root(stage_root)
    gdf = load_stage_gdf(stage_root)

    with psycopg.connect(CONFIG.database_url) as conn:
        apply_migrations(conn)
        ingest_run_id, _manifest = insert_meta(conn, raw_root, stage_root, status="publishing")
        load_raw_tables(conn, gdf, ingest_run_id)
        load_stage_and_core(conn, gdf, ingest_run_id)
        load_context(conn, stage_root)
        mark_run(conn, ingest_run_id, "published")

    print(f"Loaded MBJB run {stage_root.name} into PostGIS")


if __name__ == "__main__":
    main()
