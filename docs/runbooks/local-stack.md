# Local Stack Runbook

## Prerequisites

- Docker Desktop with Compose
- Python 3.12+
- `uv`

## Environment

Default values are in `.env.example`.

Important defaults:

- web app external port: `3001`
- PostGIS: `localhost:5432`
- pg_tileserv: `localhost:7800`

## Start the stack

```powershell
uv sync
docker compose up -d --build db tiles web
```

## Ingest, load, and QA

```powershell
.\.venv\Scripts\python.exe scripts\ingest\run_mbjb_pipeline.py --run-label mbjb_mvp
.\.venv\Scripts\python.exe scripts\publish\load_postgis.py
.\.venv\Scripts\python.exe scripts\qa\run_mbjb_qa.py
```

MBPJ text-first run:

```powershell
.\.venv\Scripts\python.exe scripts\ingest\run_mbpj_pipeline.py --run-label mbpj_phase1
.\.venv\Scripts\python.exe scripts\publish\load_mbpj_postgis.py
.\.venv\Scripts\python.exe scripts\qa\run_mbpj_qa.py
```

## Verify services

- App: `http://localhost:3001`
- Tiles: `http://localhost:7800/index.json`
- Stats API: `http://localhost:3001/api/v1/stats/overview`
- MBPJ stats API: `http://localhost:3001/api/v1/stats/overview?municipality=MBPJ`
- MBPJ search API: `http://localhost:3001/api/v1/search?q=damansara&municipality=MBPJ`
- Source page: `http://localhost:3001/sources`

## Expected behavior

- map renders MBJB development polygons from tiles
- layer toggles affect the map
- filters update both the map and the stats
- search returns known MBJB file numbers
- clicking a result or polygon opens the detail drawer
- planning block dots and labels appear when the planning-block overlay is enabled
- clicking a planning block dot opens planning block context in the detail drawer
- MBPJ data is queryable through the API as a text-first municipality even though it is not yet rendered on the public map

## Shutdown

```powershell
docker compose down
```

To remove the PostGIS volume as well:

```powershell
docker compose down -v
```
