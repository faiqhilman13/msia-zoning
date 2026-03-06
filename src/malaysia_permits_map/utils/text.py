from __future__ import annotations

import math
import re


MULTISPACE_RE = re.compile(r"\s+")
UNTUK_TETUAN_RE = re.compile(
    r"(?i)\bUNTUK\s+TETUAN\b.*?(?=\s+(?:DI|ATAS|MUKIM|DAERAH|JOHOR)\b|$)"
)
PLANNING_BLOCK_PREFIX_RE = re.compile(r"(?i)^BPK(?:\s*[:.\-]?\s*)")

MUKIM_CANONICAL_MAP = {
    "BANDAR": "Bandar Johor Bahru",
    "BANDAR JB": "Bandar Johor Bahru",
    "BANDAR JOHOR BAHRU": "Bandar Johor Bahru",
    "PELNTONG": "Plentong",
    "PENTONG": "Plentong",
    "PLENTONG": "Plentong",
    "PULAI": "Pulai",
    "SUNGAI TIRAM": "Sungai Tiram",
    "TEBRAU": "Tebrau",
}


def snake_case(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", value.strip())
    return normalized.strip("_").lower()


def clean_whitespace(value: str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return None
    return MULTISPACE_RE.sub(" ", value)


def derive_public_title(
    title: str | None,
    owner_name_raw: str | None = None,
    developer_name: str | None = None,
) -> str:
    cleaned = clean_whitespace(title) or "Maklumat pembangunan MBJB"
    cleaned = UNTUK_TETUAN_RE.sub("", cleaned)
    owner = clean_whitespace(owner_name_raw)
    if owner:
        cleaned = cleaned.replace(owner, "").replace(owner.upper(), "")
    cleaned = clean_whitespace(cleaned)
    if not cleaned:
        fallback = clean_whitespace(developer_name)
        if fallback:
            return f"Cadangan pembangunan oleh {fallback}"
        return "Maklumat pembangunan MBJB"
    return cleaned


def normalize_status(value: str | None) -> str:
    text = (clean_whitespace(value) or "").lower()
    if not text:
        return "unknown"
    if "lulus" in text:
        return "approved"
    if any(token in text for token in ("tangguh", "proses", "semakan", "mesyuarat")):
        return "pending"
    if any(token in text for token in ("tolak", "ditolak", "batal")):
        return "rejected"
    if any(token in text for token in ("tutup", "ditutup", "closed")):
        return "other"
    return "other"


def normalize_planning_block(value: str | None) -> str | None:
    text = clean_whitespace(value)
    if not text:
        return None
    text = PLANNING_BLOCK_PREFIX_RE.sub("", text).upper()
    text = text.replace("&", " & ")
    return MULTISPACE_RE.sub(" ", text).strip()


def normalize_mukim(value: str | None) -> str | None:
    text = clean_whitespace(value)
    if not text:
        return None
    upper = text.upper()
    if upper in MUKIM_CANONICAL_MAP:
        return MUKIM_CANONICAL_MAP[upper]
    return text.title()
