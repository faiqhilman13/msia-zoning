from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.etl.mbpj import (
    SOURCE_PAGES,
    MbpjClient,
    capture_source_page,
    finalize_manifest,
    make_run,
    normalize_project_register,
)
from malaysia_permits_map.etl.mbpj_geometry import ingest_geometry_layers, normalize_geometry_layers


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MBPJ ingest and normalization pipeline.")
    parser.add_argument("--run-label", default="local", help="Optional suffix for the run directory.")
    args = parser.parse_args()

    run = make_run(CONFIG, args.run_label)
    client = MbpjClient(CONFIG)
    try:
        captured_pages: dict[str, str] = {}
        for page in SOURCE_PAGES:
            captured_pages[page.slug] = capture_source_page(client, run, page)
        ingest_geometry_layers(run, CONFIG)
        normalize_project_register(run, captured_pages["homepage"])
        normalize_geometry_layers(run)
        finalize_manifest(run)
    finally:
        client.close()

    print(f"MBPJ raw artifacts: {run.raw_root}")
    print(f"MBPJ stage outputs: {run.stage_root}")
    print(f"Run ID: {run.ingest_run_id}")


if __name__ == "__main__":
    main()
