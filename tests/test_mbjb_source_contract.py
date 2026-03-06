from __future__ import annotations

import json
from pathlib import Path

from malaysia_permits_map.etl.mbjb import (
    EXPECTED_BASELINE_COUNTS,
    EXPECTED_SOURCE_EXPORT_FORMATS,
    EXPECTED_SOURCE_GEOMETRY_TYPE,
)


FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def test_mbjb_source_metadata_preserves_polygon_contract() -> None:
    metadata = json.loads(
        (FIXTURES_ROOT / "mbjb_kebenaran_merancang_layer_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["geometryType"] == EXPECTED_SOURCE_GEOMETRY_TYPE

    export_formats = {item.strip().lower() for item in metadata["supportedExportFormats"].split(",")}
    for format_name in EXPECTED_SOURCE_EXPORT_FORMATS:
        assert format_name in export_formats


def test_mbjb_baseline_counts_are_preserved() -> None:
    assert EXPECTED_BASELINE_COUNTS == {
        "pelan_bangunan": 618,
        "kebenaran_merancang": 310,
        "kerja_tanah": 190,
    }
