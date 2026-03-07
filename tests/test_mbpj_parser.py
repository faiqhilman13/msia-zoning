from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from malaysia_permits_map.etl.mbpj import PipelineRun, normalize_project_register
from malaysia_permits_map.utils.text import derive_mbpj_public_title, extract_mbpj_mukim, extract_reference_year


FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def test_normalize_project_register_parses_current_homepage_shape(tmp_path: Path) -> None:
    html = (FIXTURES_ROOT / "mbpj_homepage_approved_projects.html").read_text(encoding="utf-8")
    run = PipelineRun(
        ingest_run_id="test-run",
        started_at=datetime(2026, 3, 6, tzinfo=UTC),
        raw_root=tmp_path / "raw",
        stage_root=tmp_path / "stage",
        artifacts=[],
        observed_counts={},
    )

    frame = normalize_project_register(run, html)

    assert len(frame) == 62
    assert run.observed_counts == {"approved_project_register": 62}
    assert frame.iloc[0]["reference_no"] == "MBPJ/040100/T/P23/1/PJS5/0015/2022/SMARTDEV"
    assert frame.iloc[0]["public_display_status"] == "approved"
    assert frame["reference_no"].isna().sum() == 4
    assert frame["owner_name_raw"].notna().sum() >= 40
    assert (run.stage_root / "mbpj_project_register.parquet").exists()


def test_mbpj_text_helpers_strip_party_text_and_extract_fields() -> None:
    title = (
        "PERMOHONAN KEBENARAN MERANCANG BAGI CADANGAN MEMBINA SEBUAH RUMAH 3 TINGKAT "
        "DI ATAS LOT 574, LORONG 5/66A, SEKSYEN 5, BANDAR PETALING JAYA, DAERAH PETALING, "
        "SELANGOR DARUL EHSAN UNTUK TETUAN TAN LOKE MUN"
    )
    assert derive_mbpj_public_title(title).endswith("SELANGOR DARUL EHSAN")
    assert extract_mbpj_mukim("... MUKIM SUNGAI BULUH, DAERAH PETALING ...") == "Sungai Buloh"
    assert extract_reference_year("MBPJ/040100/T/P23/1/PJU1A/0232/2023/SMARTDEV") == 2023
