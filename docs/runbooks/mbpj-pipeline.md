# MBPJ Pipeline Runbook

## Current phase scope

MBPJ currently ships as a text-first municipality.

What this phase does:

1. capture immutable raw MBPJ HTML snapshots
2. extract the public approved-project register from the SmartDev homepage
3. normalize the rows into the canonical application model where fields exist
4. load MBPJ rows into `core.development_applications` and `core.search_documents`
5. keep geometry null until a trustworthy public geometry source is verified

What this phase does **not** do:

- publish MBPJ vector tiles
- add MBPJ polygons to the public map
- expose owner-like party text in the public UI by default

## Raw sources captured per run

- `homepage/response.html`
- `feedback_lookup/response.html`
- `terms/response.html`
- `disclaimer/response.html`
- `gis_public_authenticate/response.html`

Each captured page also gets a `response-metadata.json` file with the source URL, final URL,
status code, headers, and fetch timestamp.

## Output locations

- raw: `data/raw/mbpj/<run_name>/`
- stage: `data/stage/mbpj/<run_name>/`
- QA report: `data/publish/qa-mbpj-<run_name>.json`

## Commands

Fresh ingest:

```powershell
.\.venv\Scripts\python.exe scripts\ingest\run_mbpj_pipeline.py --run-label mbpj_phase1
```

Normalize an existing raw snapshot:

```powershell
.\.venv\Scripts\python.exe scripts\normalize\normalize_mbpj_stage.py --raw-root data\raw\mbpj\<run_name>
```

Load PostGIS:

```powershell
.\.venv\Scripts\python.exe scripts\publish\load_mbpj_postgis.py --stage-root data\stage\mbpj\<run_name>
```

QA:

```powershell
.\.venv\Scripts\python.exe scripts\qa\run_mbpj_qa.py --stage-root data\stage\mbpj\<run_name>
```

## Local API checks after load

```powershell
curl "http://localhost:3001/api/v1/stats/overview?municipality=MBPJ"
curl "http://localhost:3001/api/v1/stats/by-layer?municipality=MBPJ"
curl "http://localhost:3001/api/v1/search?q=damansara&municipality=MBPJ"
```

## Current text normalization rules

- the approved-project table source layer is normalized to `approved_project_register`
- `current_status` / `status_raw` are set to `Diluluskan`
- `public_display_status` resolves to `approved`
- `application_year` is extracted conservatively from the project reference number when present
- trailing applicant / party text is removed from `public_display_title` where detectable and stored internally as `owner_name_raw`
- `mukim` is extracted from the project title where a clear `MUKIM ...` segment exists

## Current known limitations

- MBPJ geometry is intentionally nullable in this phase
- some source rows do not expose a populated project reference number block
- applicant / owner text often appears inside the source title, so the pipeline behaves conservatively
- public redistribution rights remain unclear and should be reviewed before broader launch
