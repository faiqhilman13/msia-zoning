from __future__ import annotations

import math
import re


MULTISPACE_RE = re.compile(r"\s+")
UNTUK_TETUAN_RE = re.compile(
    r"(?i)\bUNTUK\s+TETUAN\b.*?(?=\s+(?:DI|ATAS|MUKIM|DAERAH|JOHOR)\b|$)"
)
PLANNING_BLOCK_PREFIX_RE = re.compile(r"(?i)^BPK(?:\s*[:.\-]?\s*)")
MBPJ_MUKIM_RE = re.compile(
    r"(?i)\bMUKIM\s+(.+?)(?=(?:\s*,\s*|\s*\(|\s+DAERAH\b|\s+SELANGOR\b|\s+UNTUK\b|$))"
)
MBPJ_REFERENCE_YEAR_RE = re.compile(r"/(20\d{2})/(?:SMARTDEV|DECIS)\b", re.IGNORECASE)
MBPJ_FALLBACK_YEAR_RE = re.compile(r"/(20\d{2})(?:/|$)")
MBPJ_PARTY_TAIL_MARKERS = (" UNTUK :", " UNTUK:", " UNTUK ", " TETUAN ")
MBPJ_TRAILING_PARTY_RE = re.compile(
    r"(?is)^(?P<public>.*)(?:[\s.;,:-]+)(?:UNTUK(?:\s+TETUAN)?|TETUAN)\s*:?\s*(?P<party>.+)$"
)
MBPJ_TRAILING_UNTUK_RE = re.compile(r"(?i)(?:[\s.;,:-]+)UNTUK\s*:?\s*$")
MBPJ_PARTY_NOTE_RE = re.compile(r"(?i)\s*(?:\(|,)?\s*NO\.?\s*RUJUKAN\b.*$")
MBPJ_PARTY_PREFIX_RE = re.compile(r"(?i)^TETUAN\s+")

MUKIM_CANONICAL_MAP = {
    "BANDAR": "Bandar Johor Bahru",
    "BANDAR JB": "Bandar Johor Bahru",
    "BANDAR JOHOR BAHRU": "Bandar Johor Bahru",
    "BANDAR PETALING JAYA": "Bandar Petaling Jaya",
    "DAMANSARA": "Damansara",
    "PEKAN BARU SUNGAI BULOH": "Pekan Baru Sungai Buloh",
    "PETALING": "Petaling",
    "PETALING JAYA": "Petaling Jaya",
    "PELNTONG": "Plentong",
    "PENTONG": "Plentong",
    "PLENTONG": "Plentong",
    "PULAI": "Pulai",
    "SG BULOH": "Sungai Buloh",
    "SG. BULOH": "Sungai Buloh",
    "SUNGAI BULOH": "Sungai Buloh",
    "SUNGAI BULUH": "Sungai Buloh",
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
    fallback_title: str = "Maklumat pembangunan MBJB",
) -> str:
    cleaned = clean_whitespace(title) or fallback_title
    cleaned = UNTUK_TETUAN_RE.sub("", cleaned)
    owner = clean_whitespace(owner_name_raw)
    if owner:
        cleaned = cleaned.replace(owner, "").replace(owner.upper(), "")
    cleaned = clean_whitespace(cleaned)
    if not cleaned:
        fallback = clean_whitespace(developer_name)
        if fallback:
            return f"Cadangan pembangunan oleh {fallback}"
        return fallback_title
    return cleaned


def split_trailing_party_text(title: str | None) -> tuple[str | None, str | None]:
    cleaned = clean_whitespace(title)
    if not cleaned:
        return None, None

    trailing_match = MBPJ_TRAILING_PARTY_RE.match(cleaned)
    if trailing_match:
        public_title = clean_whitespace(
            MBPJ_TRAILING_UNTUK_RE.sub("", trailing_match.group("public")).rstrip(" .,:;-")
        )
        party_text = clean_whitespace(trailing_match.group("party"))
        if party_text:
            party_text = clean_whitespace(MBPJ_PARTY_NOTE_RE.sub("", party_text))
            party_text = clean_whitespace(MBPJ_PARTY_PREFIX_RE.sub("", party_text))
        if public_title:
            return public_title, party_text

    upper = cleaned.upper()
    split_index: int | None = None
    split_marker = ""
    for marker in MBPJ_PARTY_TAIL_MARKERS:
        index = upper.rfind(marker)
        if index <= 48:
            continue
        if split_index is None or index > split_index:
            split_index = index
            split_marker = marker

    if split_index is None:
        return cleaned, None

    if split_marker == " TETUAN ":
        preceding = upper[max(0, split_index - 10) : split_index + len(split_marker)]
        untuk_index = preceding.rfind(" UNTUK ")
        if untuk_index >= 0:
            split_index = max(0, split_index - 10) + untuk_index
            split_marker = " UNTUK "

    public_title = clean_whitespace(cleaned[:split_index])
    party_text = clean_whitespace(cleaned[split_index + len(split_marker) :].lstrip(" :-,"))
    if party_text:
        party_text = clean_whitespace(MBPJ_PARTY_NOTE_RE.sub("", party_text))
        party_text = clean_whitespace(MBPJ_PARTY_PREFIX_RE.sub("", party_text))
    if not public_title:
        return cleaned, None
    return public_title, party_text


def derive_mbpj_public_title(title: str | None) -> str:
    public_title, _party_text = split_trailing_party_text(title)
    return public_title or "Maklumat projek MBPJ"


def extract_mbpj_party_text(title: str | None) -> str | None:
    _public_title, party_text = split_trailing_party_text(title)
    return party_text


def extract_reference_year(reference_no: str | None) -> int | None:
    text = clean_whitespace(reference_no)
    if not text:
        return None
    match = MBPJ_REFERENCE_YEAR_RE.search(text) or MBPJ_FALLBACK_YEAR_RE.search(text)
    if not match:
        return None
    return int(match.group(1))


def extract_mbpj_mukim(title: str | None) -> str | None:
    text = clean_whitespace(title)
    if not text:
        return None
    match = MBPJ_MUKIM_RE.search(text)
    if not match:
        return None
    return normalize_mukim(match.group(1))


def infer_application_type(title: str | None, fallback: str = "Project Register") -> str:
    text = (clean_whitespace(title) or "").upper()
    if not text:
        return fallback
    if "PELAN BANGUNAN" in text:
        return "Pelan Bangunan"
    if "KERJA TANAH" in text:
        return "Kerja Tanah"
    if "KERJA KEJURUTERAAN" in text:
        return "Kerja Kejuruteraan"
    if any(token in text for token in ("MERANCANG", "AKTA PERANCANGAN BANDAR DAN DESA", "SEKSYEN 21")):
        return "Kebenaran Merancang"
    return fallback


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
