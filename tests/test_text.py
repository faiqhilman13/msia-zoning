from __future__ import annotations

import math

from malaysia_permits_map.utils.text import (
    clean_whitespace,
    derive_public_title,
    normalize_mukim,
    normalize_planning_block,
    normalize_status,
)


def test_clean_whitespace_handles_empty_nan_and_scalars() -> None:
    assert clean_whitespace("  Lot   12  ") == "Lot 12"
    assert clean_whitespace(math.nan) is None
    assert clean_whitespace(2024) == "2024"


def test_derive_public_title_removes_owner_phrase() -> None:
    title = "Cadangan mendirikan bangunan UNTUK TETUAN ABC SDN BHD DI ATAS LOT 12"
    assert derive_public_title(title, owner_name_raw="ABC SDN BHD") == "Cadangan mendirikan bangunan DI ATAS LOT 12"


def test_normalize_status_maps_known_statuses() -> None:
    assert normalize_status("Lulus Dengan Syarat") == "approved"
    assert normalize_status("Dalam Semakan") == "pending"
    assert normalize_status("Ditolak") == "rejected"
    assert normalize_status(None) == "unknown"


def test_normalize_planning_block_removes_bpk_prefix() -> None:
    assert normalize_planning_block("BPK 18.1") == "18.1"
    assert normalize_planning_block("  bpk: 5.9&5.10 ") == "5.9 & 5.10"
    assert normalize_planning_block(None) is None


def test_normalize_mukim_fixes_case_and_common_typos() -> None:
    assert normalize_mukim("PLENTONG") == "Plentong"
    assert normalize_mukim("PELNTONG") == "Plentong"
    assert normalize_mukim("BANDAR JB") == "Bandar Johor Bahru"
    assert normalize_mukim("SUNGAI TIRAM") == "Sungai Tiram"
