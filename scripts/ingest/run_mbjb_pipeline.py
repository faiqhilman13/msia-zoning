from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_permits_map.config import CONFIG
from malaysia_permits_map.etl.arcgis_client import ArcGISClient
from malaysia_permits_map.etl.mbjb import (
    CONTEXT_LAYERS,
    DEVELOPMENT_LAYERS,
    finalize_manifest,
    ingest_layer,
    make_run,
    normalize_context_layers,
    normalize_development_layers,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MBJB ingest and normalization pipeline.")
    parser.add_argument("--run-label", default="local", help="Optional suffix for the run directory.")
    args = parser.parse_args()

    run = make_run(CONFIG, args.run_label)
    client = ArcGISClient(CONFIG)
    try:
        for layer in DEVELOPMENT_LAYERS + CONTEXT_LAYERS:
            ingest_layer(client, run, CONFIG, layer)
        finalize_manifest(run)
        context = normalize_context_layers(run)
        normalize_development_layers(run, context)
    finally:
        client.close()

    print(f"MBJB raw artifacts: {run.raw_root}")
    print(f"MBJB stage outputs: {run.stage_root}")
    print(f"Run ID: {run.ingest_run_id}")


if __name__ == "__main__":
    main()
