from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.etl.mbpj import PipelineRun, normalize_project_register
from malaysia_permits_map.etl.mbpj_geometry import normalize_geometry_layers
from malaysia_permits_map.utils.paths import ensure_directory


def latest_raw_root() -> Path:
    roots = sorted(CONFIG.mbpj_data_raw_dir.glob("*"))
    if not roots:
        raise FileNotFoundError("No raw runs found in data/raw/mbpj")
    return roots[-1]


def build_run(raw_root: Path) -> PipelineRun:
    manifest = json.loads((raw_root / "manifest.json").read_text(encoding="utf-8"))
    stage_root = ensure_directory(CONFIG.mbpj_data_stage_dir / raw_root.name)
    return PipelineRun(
        ingest_run_id=manifest["ingest_run_id"],
        started_at=datetime.fromisoformat(manifest["started_at"]),
        raw_root=raw_root,
        stage_root=stage_root,
        artifacts=[],
        observed_counts=manifest.get("observed_counts", {}),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize an existing MBPJ raw snapshot into stage Parquet.")
    parser.add_argument(
        "--raw-root",
        help="Specific raw run directory. Defaults to the latest data/raw/mbpj run.",
    )
    args = parser.parse_args()

    raw_root = Path(args.raw_root) if args.raw_root else latest_raw_root()
    run = build_run(raw_root)
    homepage_html = (raw_root / "homepage" / "response.html").read_text(encoding="utf-8")
    normalize_project_register(run, homepage_html)
    normalize_geometry_layers(run)

    print(f"MBPJ raw artifacts: {run.raw_root}")
    print(f"MBPJ stage outputs: {run.stage_root}")
    print(f"Run ID: {run.ingest_run_id}")


if __name__ == "__main__":
    main()
