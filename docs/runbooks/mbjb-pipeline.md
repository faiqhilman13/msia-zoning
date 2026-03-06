# MBJB Pipeline Runbook

## Ingestion sequence

The MBJB pipeline follows this order:

1. fetch layer metadata
2. fetch record count
3. fetch object IDs
4. batch-query features by object ID as GeoJSON
5. persist immutable raw artifacts
6. normalize to GeoParquet
7. load PostGIS core and marts
8. run QA

## Source layers

Development layers:

- `MaklumatPembangunan/3` - `KebenaranMerancang`
- `MaklumatPembangunan/1` - `PelanBangunan`
- `MaklumatPembangunan/4` - `KerjaTanah`

Context layers used in the MVP:

- `Sempadan_MBJB_Merge/1` - MBJB boundary
- `RancanganTempatan/0` - planning blocks
- `Mukim_161025/0` - mukim boundaries

## Output locations

- raw: `data/raw/mbjb/<run_name>/`
- stage: `data/stage/mbjb/<run_name>/`
- QA report: `data/publish/qa-<run_name>.json`

## Commands

Fresh ingest:

```powershell
.\.venv\Scripts\python.exe scripts\ingest\run_mbjb_pipeline.py --run-label mbjb_mvp
```

Normalize an existing raw snapshot:

```powershell
.\.venv\Scripts\python.exe scripts\normalize\normalize_mbjb_stage.py --raw-root data\raw\mbjb\<run_name>
```

Load PostGIS:

```powershell
.\.venv\Scripts\python.exe scripts\publish\load_postgis.py --stage-root data\stage\mbjb\<run_name>
```

QA:

```powershell
.\.venv\Scripts\python.exe scripts\qa\run_mbjb_qa.py --stage-root data\stage\mbjb\<run_name>
```

## Canonicalization rules

- planning block values are normalized to a single canonical value
  - example: `BPK 18.1` -> `18.1`
- mukim values are standardized for casing and obvious source typos
  - example: `PLENTONG`, `PELNTONG`, `PENTONG` -> `Plentong`
- `public_display_title` removes obvious owner phrases where possible
- `public_display_status` is bucketed into:
  - `approved`
  - `pending`
  - `rejected`
  - `other`
  - `unknown`

## Privacy controls

- `PEMILIK` is stored in raw/internal tables as `owner_name_raw`
- `owner_name_raw` is intentionally excluded from public API payloads and the public UI
- raw snapshots should be treated as internal working copies, not public distribution artifacts
