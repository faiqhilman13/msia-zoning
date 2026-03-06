# Malaysia Permits Map

MBJB-first public development map for Malaysia.

The current MVP ships a real local stack for **Majlis Bandaraya Johor Bahru (MBJB)** using public **GeoJB** ArcGIS REST data, PostGIS, `pg_tileserv`, and a Next.js frontend with MapLibre and deck.gl.

## What is implemented

- MBJB ingestion for:
  - `KebenaranMerancang` (`MaklumatPembangunan/3`)
  - `PelanBangunan` (`MaklumatPembangunan/1`)
  - `KerjaTanah` (`MaklumatPembangunan/4`)
- Immutable raw snapshots under `data/raw/mbjb/<run>/`
- Normalized GeoParquet stage outputs under `data/stage/mbjb/<run>/`
- PostGIS schemas: `meta`, `raw`, `stage`, `core`, `marts`
- Vector tiles from `pg_tileserv`
- Next.js public map with:
  - layer toggles
  - filters
  - search
  - hover tooltip
  - detail drawer
  - planning block labels and clickable planning block dots
  - summary stats
  - source page
- Conservative public-field allowlist that does **not** expose `PEMILIK` in the UI
- QA checks for counts, required fields, geometry validity, extent checks, and known references

## Stack

- Python 3.12+
- `uv`
- `httpx`, `geopandas`, `shapely`, `pyogrio`, `polars`, `duckdb`, `psycopg`
- PostgreSQL + PostGIS
- `pg_tileserv`
- Next.js + TypeScript
- MapLibre GL JS + deck.gl
- Docker Compose

## Quick start

1. Install Python dependencies:

```powershell
uv sync
```

2. Start local services:

```powershell
docker compose up -d --build db tiles web
```

3. Run the MBJB ingest and normalization pipeline:

```powershell
.\.venv\Scripts\python.exe scripts\ingest\run_mbjb_pipeline.py --run-label mbjb_mvp
```

4. Load the latest stage output into PostGIS:

```powershell
.\.venv\Scripts\python.exe scripts\publish\load_postgis.py
```

5. Run QA:

```powershell
.\.venv\Scripts\python.exe scripts\qa\run_mbjb_qa.py
```

6. Open the app:

- Web app: `http://localhost:3001`
- Tile index: `http://localhost:7800/index.json`

## Common commands

Run tests:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Re-normalize an existing raw snapshot without re-downloading:

```powershell
.\.venv\Scripts\python.exe scripts\normalize\normalize_mbjb_stage.py --raw-root data\raw\mbjb\<run_name>
```

Load a specific stage run:

```powershell
.\.venv\Scripts\python.exe scripts\publish\load_postgis.py --stage-root data\stage\mbjb\<run_name>
```

## Repo layout

- `app/web` - Next.js frontend and API routes
- `scripts/ingest` - source download pipeline
- `scripts/normalize` - stage materialization from raw snapshots
- `scripts/publish` - PostGIS load
- `scripts/qa` - QA checks
- `infra/sql` - reserved for future shared SQL assets
- `infra/migrations` - schema and marts
- `data/raw` - immutable source snapshots
- `data/stage` - normalized GeoParquet
- `docs` - runbooks, attribution, and data notes

## Source facts preserved in code and docs

- MBJB development records are already polygon geometry.
- The GeoJB layer contract supports export formats including `geojson`, `csv`, and `shapefile`.
- Current observed public counts used for the MBJB baseline:
  - `PelanBangunan`: `618`
  - `KebenaranMerancang`: `310`
  - `KerjaTanah`: `190`

## Public data handling

- Raw source snapshots are kept on disk for reproducibility and auditability.
- The public app serves tiles and reviewed API responses, not raw source dumps.
- Internal-only raw fields such as `owner_name_raw` are retained in storage but not shown in the public UI by default.

## Documentation

- [Local stack runbook](./docs/runbooks/local-stack.md)
- [MBJB pipeline runbook](./docs/runbooks/mbjb-pipeline.md)
- [Data dictionary](./docs/data-dictionary.md)
- [Source attribution and licensing notes](./docs/source-attribution.md)
