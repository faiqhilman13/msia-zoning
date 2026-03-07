from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import orjson
import pandas as pd
from bs4 import BeautifulSoup

from malaysia_permits_map.config import AppConfig
from malaysia_permits_map.utils.paths import ensure_directory
from malaysia_permits_map.utils.text import (
    clean_whitespace,
    derive_mbpj_public_title,
    extract_mbpj_mukim,
    extract_mbpj_party_text,
    extract_reference_year,
    infer_application_type,
    normalize_status,
    snake_case,
)


SOURCE_SYSTEM = "mbpj_smartdev"
MUNICIPALITY = "MBPJ"
SOURCE_LAYER = "approved_project_register"
NAMESPACE = uuid.UUID("28b31545-c9df-4a8d-9f69-0ff5f2d9858b")
EXPECTED_PROJECT_COUNT = 62


@dataclass(frozen=True)
class SourcePageConfig:
    slug: str
    url: str


SOURCE_PAGES: tuple[SourcePageConfig, ...] = (
    SourcePageConfig(slug="homepage", url="https://mbpjsmartdev.pjsmartcity.gov.my/"),
    SourcePageConfig(
        slug="feedback_lookup",
        url="https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas",
    ),
    SourcePageConfig(
        slug="terms",
        url="https://mbpjsmartdev.pjsmartcity.gov.my/pages/terma-_-syarat",
    ),
    SourcePageConfig(
        slug="disclaimer",
        url="https://mbpjsmartdev.pjsmartcity.gov.my/pages/penafian",
    ),
    SourcePageConfig(
        slug="gis_public_authenticate",
        url="http://pjcityplan.mbpj.gov.my:81/mapguide/mbpjfusion/public_authhenticate.php",
    ),
)


@dataclass
class SourceArtifact:
    artifact_type: str
    relative_path: str
    sha256: str
    file_size_bytes: int


@dataclass
class PipelineRun:
    ingest_run_id: str
    started_at: datetime
    raw_root: Path
    stage_root: Path
    artifacts: list[SourceArtifact]
    observed_counts: dict[str, int]

    @property
    def manifest_path(self) -> Path:
        return self.raw_root / "manifest.json"


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def make_run(config: AppConfig, run_label: str | None = None) -> PipelineRun:
    started_at = utc_now()
    run_stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    if run_label:
        run_stamp = f"{run_stamp}_{snake_case(run_label)}"
    raw_root = ensure_directory(config.mbpj_data_raw_dir / run_stamp)
    stage_root = ensure_directory(config.mbpj_data_stage_dir / run_stamp)
    return PipelineRun(
        ingest_run_id=str(uuid.uuid4()),
        started_at=started_at,
        raw_root=raw_root,
        stage_root=stage_root,
        artifacts=[],
        observed_counts={},
    )


class MbpjClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = httpx.Client(
            timeout=config.mbpj_request_timeout_seconds,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def _request(self, url: str) -> httpx.Response:
        last_error: Exception | None = None
        for _attempt in range(1, self.config.mbpj_retry_attempts + 1):
            try:
                response = self.client.get(url)
                response.raise_for_status()
                return response
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError(f"Request failed for {url}") from last_error

    def fetch_page(self, page: SourcePageConfig) -> httpx.Response:
        return self._request(page.url)


def _write_bytes(path: Path, content: bytes, artifact_type: str | None = None) -> SourceArtifact:
    ensure_directory(path.parent)
    path.write_bytes(content)
    return SourceArtifact(
        artifact_type=artifact_type or (path.suffix.lstrip(".") or "file"),
        relative_path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        file_size_bytes=len(content),
    )


def write_json(path: Path, payload: dict[str, Any]) -> SourceArtifact:
    content = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    return _write_bytes(path, content, artifact_type="json")


def capture_source_page(client: MbpjClient, run: PipelineRun, page: SourcePageConfig) -> str:
    response = client.fetch_page(page)
    page_root = ensure_directory(run.raw_root / page.slug)
    html_path = page_root / "response.html"
    metadata_path = page_root / "response-metadata.json"

    run.artifacts.append(_write_bytes(html_path, response.content, artifact_type="html"))
    run.artifacts.append(
        write_json(
            metadata_path,
            {
                "slug": page.slug,
                "source_url": page.url,
                "final_url": str(response.url),
                "status_code": response.status_code,
                "fetched_at": utc_now().isoformat(),
                "content_type": response.headers.get("content-type"),
                "headers": dict(response.headers),
                "sha256": hashlib.sha256(response.content).hexdigest(),
            },
        )
    )

    encoding = response.encoding or "utf-8"
    return response.content.decode(encoding, errors="replace")


def _stable_source_object_id(source_key: str) -> int:
    digest = hashlib.sha256(source_key.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def _stable_application_id(source_object_id: int) -> str:
    natural_key = f"{SOURCE_SYSTEM}|{SOURCE_LAYER}|{source_object_id}"
    return str(uuid.uuid5(NAMESPACE, natural_key))


def _parse_project_rows(homepage_html: str, run: PipelineRun) -> list[dict[str, Any]]:
    soup = BeautifulSoup(homepage_html, "html.parser")
    table = soup.select_one("table#sample_4")
    if table is None:
        raise ValueError("MBPJ approved project table #sample_4 was not found in the homepage snapshot")

    rows: list[dict[str, Any]] = []
    seen_source_ids: set[int] = set()
    for row in table.select("tbody > tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 2:
            continue

        row_number_text = clean_whitespace(cells[0].get_text(" ", strip=True))
        row_number = int((row_number_text or "0").rstrip("."))

        content_soup = BeautifulSoup(str(cells[1]), "html.parser")
        reference_block = content_soup.select_one("div.form-group")
        reference_no = clean_whitespace(reference_block.get_text(" ", strip=True)) if reference_block else None
        if reference_block is not None:
            reference_block.extract()

        title = clean_whitespace(content_soup.get_text(" ", strip=True))
        public_display_title = derive_mbpj_public_title(title)
        party_text = extract_mbpj_party_text(title)
        source_key = reference_no or public_display_title or f"row-{row_number}"
        source_object_id = _stable_source_object_id(source_key)
        if source_object_id in seen_source_ids:
            source_object_id = _stable_source_object_id(f"{source_key}|{row_number}")
        seen_source_ids.add(source_object_id)

        raw_payload = {
            "row_number": row_number,
            "reference_no": reference_no,
            "title": title,
            "public_display_title": public_display_title,
            "party_text": party_text,
            "source_key": source_key,
            "source_url": SOURCE_PAGES[0].url,
            "row_html": str(row),
        }

        rows.append(
            {
                "application_id": _stable_application_id(source_object_id),
                "ingest_run_id": run.ingest_run_id,
                "source_system": SOURCE_SYSTEM,
                "source_municipality": MUNICIPALITY,
                "source_layer": SOURCE_LAYER,
                "source_object_id": source_object_id,
                "reference_no": reference_no,
                "reference_no_alt": None,
                "title": title,
                "application_type": infer_application_type(title),
                "current_status": "Diluluskan",
                "status_raw": "Diluluskan",
                "application_year": extract_reference_year(reference_no),
                "approval_year": None,
                "meeting_date_1": None,
                "meeting_decision_1": None,
                "meeting_date_2": None,
                "meeting_decision_2": None,
                "meeting_date_3": None,
                "meeting_decision_3": None,
                "meeting_date_4": None,
                "meeting_decision_4": None,
                "lot_no": None,
                "mukim": extract_mbpj_mukim(title),
                "planning_block": None,
                "zoning_name": None,
                "owner_name_raw": party_text,
                "developer_name": None,
                "consultant_name": None,
                "proxy_holder_name": None,
                "site_area_acres": None,
                "site_area_m2": None,
                "area_m2": None,
                "area_acres": None,
                "public_display_title": public_display_title,
                "public_display_status": normalize_status("Diluluskan"),
                "is_public_visible": True,
                "raw_record_hash": hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode("utf-8")).hexdigest(),
                "raw_attributes": raw_payload,
                "source_row_number": row_number,
                "source_url": SOURCE_PAGES[0].url,
                "geometry": None,
                "centroid": None,
                "created_at": run.started_at,
                "updated_at": run.started_at,
            }
        )
    return rows


def normalize_project_register(run: PipelineRun, homepage_html: str) -> pd.DataFrame:
    rows = _parse_project_rows(homepage_html, run)
    run.observed_counts[SOURCE_LAYER] = len(rows)

    extract_root = ensure_directory(run.raw_root / SOURCE_LAYER)
    ensure_directory(run.stage_root)
    run.artifacts.append(write_json(extract_root / "projects-extracted.json", {"rows": rows}))

    frame = pd.DataFrame(rows)
    frame.to_parquet(run.stage_root / "mbpj_project_register.parquet", index=False)
    return frame


def finalize_manifest(run: PipelineRun) -> None:
    write_json(
        run.manifest_path,
        {
            "ingest_run_id": run.ingest_run_id,
            "source_system": SOURCE_SYSTEM,
            "municipality_code": MUNICIPALITY,
            "started_at": run.started_at.isoformat(),
            "observed_counts": run.observed_counts,
            "geometry_status": "context_geometry_only",
            "geometry_notes": "MBPJ ArcGIS context geometry was captured, but no direct public project geometry was linked to SmartDev project rows.",
            "expected_project_count": EXPECTED_PROJECT_COUNT,
            "artifacts": [asdict(artifact) for artifact in run.artifacts],
        },
    )
