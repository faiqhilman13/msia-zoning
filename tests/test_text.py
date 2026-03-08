from __future__ import annotations

import math

from malaysia_permits_map.utils.text import (
    clean_whitespace,
    derive_mbpj_public_title,
    derive_public_title,
    extract_mbpj_party_text,
    infer_application_type,
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


def test_mbpj_title_helpers_strip_trailing_party_markers_and_notes() -> None:
    title = (
        "PERMOHONAN KEBENARAN MERANCANG BAGI CADANGAN PEMBANGUNAN DI ATAS LOT 81117, "
        "ARA DAMANSARA, MUKIM DAMANSARA, DAERAH PETALING, SELANGOR DARUL EHSAN. UNTUK: "
        "LUSTER ARA SDN. BHD. (NO RUJUKAN PELAN LULUS: MBPJ/TEST/123)"
    )
    assert derive_mbpj_public_title(title).endswith("SELANGOR DARUL EHSAN")
    assert extract_mbpj_party_text(title) == "LUSTER ARA SDN. BHD."


def test_infer_application_type_handles_planning_variants_and_typos() -> None:
    assert infer_application_type("PERMOHONAN MERANCANG BAGI CADANGAN MEMBINA RUMAH") == "Kebenaran Merancang"
    assert infer_application_type("PERMOHONAN KENENARAN MERANCANG BAGI CADANGAN TAMBAHAN") == "Kebenaran Merancang"
