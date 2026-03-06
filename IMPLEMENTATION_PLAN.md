# Malaysia Permits Map

Implementation plan for a public-facing geospatial product that visualizes development approvals and related planning data from Malaysian municipalities, starting with a municipality that already exposes usable public machine-readable data.

Last updated: March 6, 2026

## 0. Implementation Update: March 6, 2026

The MBJB MVP described in this plan is now implemented in this repository as a runnable local stack.

Key implementation decisions captured in code and docs:

- default external web port is `3001` to avoid common local port conflicts on `3000`
- the frontend is served by a Next.js app in `app/web`
- vector tiles are served from `pg_tileserv` against `marts.mbjb_public_features`
- the MVP uses these context layers in addition to the 3 development layers:
  - `Sempadan_MBJB_Merge/1`
  - `RancanganTempatan/0`
  - `Mukim_161025/0`
- raw source artifacts are preserved under `data/raw/mbjb/<run>/`
- normalized GeoParquet outputs are written to `data/stage/mbjb/<run>/`
- an explicit normalization step is available in `scripts/normalize/normalize_mbjb_stage.py`
- canonicalization now standardizes:
  - planning block values such as `BPK 18.1` -> `18.1`
  - mukim casing and obvious source variants such as `PLENTONG`, `PELNTONG`, `PENTONG` -> `Plentong`
- the public app intentionally excludes raw owner fields such as `PEMILIK`

Current implementation status as of March 6, 2026:

- MBJB ingest, normalize, load, and QA scripts are implemented and runnable end to end
- raw snapshots, stage GeoParquet, and PostGIS publish tables are all present in the working stack
- the frontend now includes:
  - layer toggles
  - filters
  - search
  - hover tooltip
  - feature detail drawer
  - planning block labels and clickable planning block dots
  - source attribution page
- the planning block overlay and its point markers now react to the same filter state as the stats and tiles
- a tile URL templating bug that previously prevented development polygons from rendering was fixed on March 6, 2026
- map readability was improved on March 6, 2026 by:
  - strengthening permit polygon contrast
  - pushing planning block context further into the background
  - improving dark tooltip text contrast
- the remaining known frontend issue is a non-blocking `loaders.gl` deprecation warning about `options.gis`

Verification completed in this repository:

- `pytest` passes for the current test suite
- `npm run build` passes for the Next.js app
- local Docker Compose startup path has been verified
- filtered stats and filtered map behavior have been verified together in the running browser

Strategic status:

- MBJB MVP is complete enough to serve as the baseline municipality
- the next municipality to implement should now be `MBPJ`

## 1. Executive Summary

### Goal

Build an interactive municipal development map for Malaysia that lets a user:

- explore approved and in-process development records spatially
- filter by approval type, status, year, planning block, and other metadata
- inspect polygons and their attributes on hover and click
- understand the development context using zoning, land-use, and boundary overlays
- support future expansion to more municipalities without redesigning the system

### Recommended first municipality

Start with **Majlis Bandaraya Johor Bahru (MBJB)**.

Reason:

- MBJB already exposes public geospatial development data through `GeoJB`
- the data is queryable through public ArcGIS REST services
- the development records are already polygons, which removes the hardest MVP problem: address geocoding to site geometry
- the exposed layers are already split into the exact kinds of records we care about:
  - `PelanBangunan`
  - `KebenaranMerancang`
  - `KerjaTanah`

### MVP definition

The MVP is a production-grade but tightly scoped product for one municipality:

- municipality: `MBJB`
- public web map
- 3 development layers rendered as polygons
- filterable stats and search
- zoning and administrative context layers
- stable ingestion pipeline
- tile-based serving
- documented, repeatable deployment

The MVP does **not** need to solve nationwide permit ingestion, Malaysian address geocoding, or perfect building-footprint matching on day one.

## 2. Why This Project Exists

### Problem

Malaysian municipal development data is fragmented:

- some councils expose forms only
- some expose zoning only
- some expose project lists without geometry
- some expose geospatial data but not in a productized, searchable experience

As a result, the public and even internal users often cannot answer simple questions quickly:

- what has been approved in this area?
- what kind of development is it?
- when was it approved?
- who is the developer or consultant?
- which planning block, zoning area, or mukim does it fall in?
- what is happening around a specific lot or neighborhood?

### Product thesis

If we begin with a municipality that already exposes polygon-based development records, we can:

- deliver a useful MVP quickly
- prove product value before tackling harder data problems
- build the ingestion and map stack once
- later expand to municipalities with weaker data using scraping, zoning overlays, and eventually geocoding

## 3. Users and Use Cases

### Primary users

- public users monitoring development activity
- urban researchers and journalists
- planners and GIS users
- developers and consultants doing location intelligence
- civic-tech users interested in transparency

### Core user stories

- As a user, I can pan to an area and see development polygons.
- As a user, I can toggle development layers on and off.
- As a user, I can filter to planning permissions only.
- As a user, I can search by file number, lot number, mukim, developer, or project title.
- As a user, I can click a polygon and inspect structured metadata.
- As a user, I can understand the development against zoning and boundaries.
- As an operator, I can rerun ingestion and detect what changed.

## 4. Scope and Sequencing

### Phase 0 scope decision

Build a municipal-first product, not a Malaysia-first product.

### Municipality order of execution

1. `MBJB` - strongest public machine-readable source
2. `MBPJ` - public approved-project list, likely scrapeable, plus public GIS
3. `Penang` - strong open geospatial context data
4. `MBSP` - strong zoning context
5. `Selangor PBTs via SISMAPS` - context and zoning, not necessarily open bulk permit exports
6. `DBKL` - planning controls and geospatial references, permit records still unclear as open bulk data

### MVP includes

- MBJB development polygon ingestion
- MBJB base contextual layers
- canonical normalized schema
- spatial database
- vector tile serving
- map frontend
- operator documentation

### MVP excludes

- nationwide permit coverage
- full ETL for every municipality
- authenticated user roles
- user-contributed edits
- automated acquisition from private or captcha-protected systems
- public display of sensitive fields without review

## 5. Source Inventory

This section distinguishes between:

- **primary MVP data**: directly powers the first map
- **secondary expansion data**: useful for future municipalities or enrichment
- **reference and support data**: zoning, boundaries, footprints, and context

## 5.1 Primary MVP Data Sources

### A. MBJB GeoJB

Portal:

- `https://geojb.gov.my/gisportal/apps/sites/#/mbjb-geojb`

Service root:

- `https://geojb.gov.my/gisserver/rest/services?f=pjson`
- `https://geojb.gov.my/gisserver/rest/services/GEOJB?f=pjson`

Target service:

- `https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson`

Layers:

- `PelanBangunan` layer id `1`
- `KebenaranMerancang` layer id `3`
- `KerjaTanah` layer id `4`

Current public counts observed on March 6, 2026:

- `PelanBangunan`: `618`
- `KebenaranMerancang`: `310`
- `KerjaTanah`: `190`

Data type:

- polygons

Important known fields from `KebenaranMerancang`:

- `No_Fail`
- `Tajuk_Fail`
- `STATUS_PERMOHONAN_SEMASA`
- `NO_LOT`
- `MUKIM`
- `BLOK_PERANCANGAN`
- `PEMILIK`
- `PEMAJU`
- `PEMEGANG_PA`
- `PERUNDING`
- `LUAS_EKAR`
- `TAHUN_LULUS`
- `Tahun_Mohon`
- several decision and meeting date fields

Technical notes:

- supports `geojson`, `csv`, `shapefile`, and `filegdb` export
- supports `Query`
- public REST service is directly callable
- geometry already present, so geocoding is not required for MVP

Role in system:

- primary record source for the first released product

### B. MBJB planning context layers

Also available from GeoJB:

- `RancanganTempatan`
- `SempadanPentadbiran`
- `LotJUPEM`
- `Bangunan`
- `Infrastruktur`
- and many others

Role in system:

- zoning overlays
- boundary overlays
- parcel or lot context
- optional building context

## 5.2 Strong Secondary Expansion Sources

### A. MBPJ SmartDev

Portal:

- `https://mbpjsmartdev.pjsmartcity.gov.my/`

Useful public pages:

- approved project list on homepage
- feedback lookup page:
  - `https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas`
- GIS portal:
  - `http://pjcityplan.mbpj.gov.my:81/mapguide/mbpjfusion/main.php`

Observed characteristics:

- homepage displays a public `Senarai Projek Yang Diluluskan`
- the UI showed `62` approved-project entries at the time checked
- each row includes a project reference and long project title with location context
- public GIS portal exposes land-use and administrative context

Role in system:

- phase 2 municipality
- scraping target for non-machine-readable but public project records
- reconciliation source against zoning and lot context

### B. Penang GIS Open Data

Portal:

- `https://data-pegis.opendata.arcgis.com/`

Observed characteristics:

- explicit open geospatial data hub
- categories include:
  - `Built Environment`
  - `Landuse`
  - `Demarcation`
  - `Transportation`
  - `Government`

Role in system:

- contextual overlays
- candidate second-state expansion
- open-data enrichment source

### C. MBSP SPGIS

Official page:

- `https://www.mbsp.gov.my/index.php/en/form/town-planning`

Linked GIS:

- `https://spgis.mbsp.gov.my/portal/home/`
- `https://spgis.mbsp.gov.my/portal/apps/webappviewer/index.html?id=c89c649700a242ef894f4d0997f09e9d`

Role in system:

- zoning context for Seberang Perai
- future municipality support

### D. Selangor SISMAPS

Info page:

- `https://www.jpbdselangor.gov.my/index.php/en/perkhidmatan/sistem-maklumat-geografi-gis/sistem-maklumat-perancangan-negeri-selangor-sismaps`

Portal:

- `https://sismaps.jpbdselangor.gov.my/index`

Role in system:

- statewide zoning and planning context
- future reference for Selangor municipalities

### E. DBKL CPS / GeoKL

Portal:

- `https://cps.dbkl.gov.my/`

Role in system:

- Kuala Lumpur planning controls, lot-level context, and zoning information
- useful for future KL expansion once permit record strategy is clearer

## 5.3 National and Cross-Cutting Support Sources

### A. KPKT / data.gov.my datasets

Useful examples:

- housing development licensing and advertising-sales permit datasets
- local plan registries
- state housing project lists

Role in system:

- metadata enrichment
- cross-checking developer and project references
- future aggregation beyond a single municipality

### B. Building footprints

Potential sources:

- Microsoft Global ML Building Footprints
- Google Open Buildings
- OSM / Geofabrik extracts

Role in system:

- future building-based extrusion
- map context
- fallback geometry where permit data is point-based in other municipalities

## 6. Data Access and Licensing Notes

This project must treat **publicly accessible** and **openly licensed** as different things.

### Policy

- If a portal explicitly brands itself as open data, we can treat it as an open-data candidate, but still capture attribution and terms.
- If a portal is publicly accessible but not explicitly open-licensed, we can use it for prototyping and internal development, but public redistribution must be reviewed.
- Any field that may contain personally identifying or sensitive information must be reviewed before public display.

### Immediate caution areas

- `PEMILIK` may represent a private owner name
- some project titles may embed owner, consultant, or lot details that should not be overexposed
- MBPJ feedback/lookup flows may imply public access but not necessarily republication rights

### Safe default for MVP

- ingest all fields internally
- publicly expose only a reviewed allowlist
- hide or redact owner names by default
- keep raw source snapshots private

## 7. Product Definition for the MVP

### The map must show

- development polygons from MBJB
- 3 separate layers:
  - planning permission
  - building plan
  - earthworks
- basic cartographic context:
  - municipality boundary
  - planning block or zoning where available
  - roads and basemap

### The map must support

- search
- filtering by layer
- filtering by status
- filtering by year
- filtering by planning block and mukim
- hover tooltip
- detail drawer
- legend
- statistics summary

### The operator system must support

- rerunning ingestion
- comparing record counts to prior runs
- exporting normalized GeoParquet and loading PostGIS
- tracking raw snapshots per run

## 8. Recommended Technical Stack

## 8.1 Backend and data

- `Python 3.12`
- `uv` for dependency management
- `requests` or `httpx` for ArcGIS REST ingestion
- `polars` for tabular transforms
- `geopandas`, `shapely`, `pyogrio` for geospatial transforms
- `duckdb` for fast local QA and parquet inspection
- `PostgreSQL 16 + PostGIS`
- `pg_tileserv` for vector tile serving in MVP

## 8.2 Frontend

- `Next.js`
- `TypeScript`
- `MapLibre GL JS`
- `deck.gl`
- a lightweight UI layer for filters and drawers

## 8.3 Infra

- `Docker Compose` for local development
- one managed Postgres/PostGIS instance or VPS-hosted PostGIS
- app deployment on `Fly.io`, `Railway`, `Render`, or a VPS

## 8.4 Why this stack

- ArcGIS REST is easy to consume with Python
- PostGIS provides robust spatial joins and indexing
- `pg_tileserv` minimizes custom tile-server work
- `MapLibre + deck.gl` gives flexible 2D and 3D rendering

## 9. Architecture

```text
Public Sources
  |- MBJB GeoJB ArcGIS REST
  |- MBJB context layers
  |- MBPJ / Penang / MBSP / SISMAPS (future)
         |
         v
Ingestion Layer
  |- source metadata fetch
  |- id pagination
  |- batch feature download
  |- raw snapshot storage
         |
         v
Normalization Layer
  |- field mapping
  |- date parsing
  |- geometry repair
  |- CRS normalization
  |- canonical schema
         |
         v
Spatial Warehouse
  |- Postgres/PostGIS
  |- raw schema
  |- stage schema
  |- core schema
  |- marts schema
         |
         +--> pg_tileserv
         |
         +--> app API
                 |
                 v
Frontend
  |- Next.js
  |- MapLibre
  |- deck.gl
  |- filter/search/detail UI
```

## 10. Repository Layout

Recommended repository layout inside this folder once implementation begins:

```text
malaysia-permits-map/
  IMPLEMENTATION_PLAN.md
  README.md
  .env.example
  docker-compose.yml
  app/
    web/
  data/
    raw/
    snapshots/
    stage/
    publish/
  docs/
    data-dictionary/
    runbooks/
  infra/
    sql/
    migrations/
    tiles/
  scripts/
    ingest/
    normalize/
    publish/
    qa/
  tests/
    unit/
    integration/
```

## 11. Data Architecture

## 11.1 Storage zones

### Raw zone

Purpose:

- preserve source responses exactly as received
- support auditability and replay

Artifacts:

- raw layer metadata JSON
- raw query responses
- raw feature collections
- per-run manifest

Example path:

```text
data/raw/mbjb/2026-03-06/kebenaran_merancang/
```

### Stage zone

Purpose:

- normalized but still source-oriented data

Artifacts:

- GeoParquet files
- cleaned field names
- normalized dates
- repaired geometries

### Publish zone

Purpose:

- app-ready data
- optimized for API, tiles, and analytics

Artifacts:

- PostGIS tables
- materialized views
- optional static exports

## 11.2 Database schemas

Recommended schemas:

- `meta`
- `raw`
- `stage`
- `core`
- `marts`

### meta schema

Tables:

- `meta.source_registry`
- `meta.ingest_runs`
- `meta.ingest_artifacts`
- `meta.schema_versions`
- `meta.quality_checks`

### raw schema

Tables:

- `raw.mbjb_pelan_bangunan`
- `raw.mbjb_kebenaran_merancang`
- `raw.mbjb_kerja_tanah`
- `raw.mbjb_zoning`
- `raw.mbjb_boundaries`

### stage schema

Tables:

- `stage.mbjb_development_unified`
- `stage.mbjb_context_layers`

### core schema

Tables:

- `core.development_applications`
- `core.development_geometries`
- `core.zoning_areas`
- `core.admin_boundaries`
- `core.search_documents`

### marts schema

Tables or materialized views:

- `marts.stats_overview`
- `marts.stats_by_layer`
- `marts.stats_by_status`
- `marts.stats_by_year`
- `marts.stats_by_planning_block`

## 12. Canonical Data Model

The source schemas will differ by municipality. The application needs a canonical model.

## 12.1 Canonical application table

Suggested fields:

- `application_id` UUID
- `source_system`
- `source_municipality`
- `source_layer`
- `source_object_id`
- `reference_no`
- `reference_no_alt`
- `title`
- `application_type`
- `current_status`
- `status_raw`
- `application_year`
- `approval_year`
- `meeting_date_1`
- `meeting_decision_1`
- `meeting_date_2`
- `meeting_decision_2`
- `meeting_date_3`
- `meeting_decision_3`
- `meeting_date_4`
- `meeting_decision_4`
- `lot_no`
- `mukim`
- `planning_block`
- `zoning_name`
- `owner_name_raw`
- `developer_name`
- `consultant_name`
- `proxy_holder_name`
- `site_area_acres`
- `site_area_m2`
- `public_display_title`
- `public_display_status`
- `is_public_visible`
- `raw_record_hash`
- `ingest_run_id`
- `created_at`
- `updated_at`

### Notes

- `owner_name_raw` should be stored internally and reviewed before public display.
- `public_display_title` is a derived safe field for the frontend.

## 12.2 Geometry table

Suggested fields:

- `geometry_id` UUID
- `application_id`
- `geometry`
- `geometry_type`
- `centroid`
- `bbox`
- `area_m2`
- `area_acres`
- `is_valid_geometry`
- `geometry_source`

## 12.3 Search table

Suggested fields:

- `search_id`
- `application_id`
- `reference_no`
- `title`
- `developer_name`
- `lot_no`
- `mukim`
- `planning_block`
- `tsvector`

## 13. Ingestion Pipeline Design

## 13.1 MBJB ingestion strategy

MBJB uses ArcGIS REST, so the safest pipeline is:

1. fetch layer metadata
2. fetch total count
3. fetch all object IDs
4. batch query by object IDs
5. store raw JSON snapshots
6. normalize to GeoParquet
7. load to PostGIS

### Why not scrape the map UI

- the UI is unnecessary overhead
- the REST endpoints are already public
- query endpoints are more stable than HTML or JS scraping

## 13.2 ArcGIS REST extraction pattern

Example pattern for `KebenaranMerancang`:

```text
GET /FeatureServer/3?f=pjson
GET /FeatureServer/3/query?where=1=1&returnCountOnly=true&f=pjson
GET /FeatureServer/3/query?where=1=1&returnIdsOnly=true&f=pjson
GET /FeatureServer/3/query?objectIds=<chunk>&outFields=*&returnGeometry=true&f=geojson
```

### Extraction rules

- chunk object IDs in batches of `200` to `500`
- retry failed requests with exponential backoff
- record HTTP status and response hash
- never overwrite raw snapshots
- capture layer metadata per run in case schema changes

## 13.3 Raw artifact naming

```text
data/raw/mbjb/2026-03-06/kebenaran_merancang/
  layer-metadata.json
  ids.json
  batch-0001.geojson
  batch-0002.geojson
  manifest.json
```

## 13.4 Normalization steps

For each layer:

1. read all GeoJSON batches
2. merge to a single GeoDataFrame
3. standardize column names to snake_case
4. map source field names to canonical names
5. convert ArcGIS dates to UTC timestamps
6. transform geometry to `EPSG:4326`
7. repair invalid geometry using `make_valid`
8. compute area and centroid
9. generate row hash
10. write GeoParquet

## 13.5 Canonical field mapping for MBJB KebenaranMerancang

Example mapping:

- `No_Fail` -> `reference_no`
- `Tajuk_Fail` -> `title`
- `STATUS_PERMOHONAN_SEMASA` -> `current_status`
- `NO_LOT` -> `lot_no`
- `MUKIM` -> `mukim`
- `BLOK_PERANCANGAN` -> `planning_block`
- `PEMILIK` -> `owner_name_raw`
- `PEMAJU` -> `developer_name`
- `PERUNDING` -> `consultant_name`
- `PEMEGANG_PA` -> `proxy_holder_name`
- `LUAS_EKAR` -> `site_area_acres`
- `TAHUN_LULUS` -> `approval_year`
- `Tahun_Mohon` -> `application_year`

## 13.6 Load to PostGIS

Load strategy:

- truncate and reload stage tables for the current municipality snapshot
- merge into core tables using `(source_system, source_layer, source_object_id)` as the natural key

Recommended indexes:

- `btree` on reference number fields
- `btree` on municipality, layer, year, status
- `gist` on geometry and centroid
- `gin` on full-text search vector

## 14. Transformation and Enrichment Pipeline

## 14.1 Required enrichments

For MVP:

- compute polygon area in square meters and acres
- derive point-on-surface for label/search
- derive a safe public title field
- normalize statuses into a smaller app taxonomy
- join planning polygons to administrative boundaries where possible

## 14.2 Status normalization

Create a small app-facing status taxonomy:

- `approved`
- `pending`
- `rejected`
- `unknown`
- `other`

Source values stay preserved in `status_raw`.

## 14.3 Public-safe field derivation

Create `public_display_title`:

- prefer cleaned `title`
- remove obvious PII patterns if necessary
- preserve useful project context

Create `is_public_visible`:

- `true` only if the record passes visibility rules

## 14.4 Spatial joins

Join development polygons to:

- municipality boundary
- mukim boundary
- planning block
- zoning layer

PostGIS functions likely used:

- `ST_Transform`
- `ST_MakeValid`
- `ST_PointOnSurface`
- `ST_Area`
- `ST_Intersects`
- `ST_Contains`
- `ST_Intersection`

## 15. Serving Strategy

## 15.1 Why tiles from day one

Even though MBJB counts are still manageable, we should not build the MVP around raw GeoJSON downloads because:

- the system must scale to additional municipalities
- map performance will stay predictable
- filtering and layer toggling will be easier

## 15.2 Tile server

Recommended MVP tile server:

- `pg_tileserv`

Reason:

- minimal operational overhead
- direct integration with PostGIS
- fast setup for MVP

## 15.3 Tile layers

Tile endpoints should expose:

- `development_km`
- `development_building_plan`
- `development_earthworks`
- `zoning`
- `boundaries`

## 15.4 API layer

In addition to tiles, expose application APIs for:

- metadata
- stats
- search
- feature details

Suggested API surface:

- `/api/v1/metadata/sources`
- `/api/v1/stats/overview`
- `/api/v1/stats/by-layer`
- `/api/v1/stats/by-status`
- `/api/v1/stats/by-year`
- `/api/v1/search?q=...`
- `/api/v1/features/:application_id`

## 16. Frontend Product Design

## 16.1 Map stack

- `MapLibre GL JS` for the base map
- `deck.gl` for overlay rendering and interactivity

## 16.2 Rendering model

Use polygon layers with symbolic 3D extrusion.

Important:

- the extrusion height in MVP is **not** real building height unless the source explicitly provides height
- use symbolic height based on category, area quantiles, or status

Suggested MVP extrusion:

- `KebenaranMerancang` -> low-medium height
- `PelanBangunan` -> medium height
- `KerjaTanah` -> low height

Or:

- use normalized site area for height, with clamped min/max values

## 16.3 Color scheme

Use color by development layer or status:

- planning permission -> green
- building plan -> blue
- earthworks -> orange

Optional status override:

- approved -> green
- pending -> yellow
- rejected -> red
- unknown -> gray

## 16.4 UI layout

Recommended layout:

- left panel:
  - municipality selector
  - layer toggles
  - filters
  - search
- map center
- right drawer:
  - clicked feature details
- top summary bar:
  - totals and filtered counts

## 16.5 Search behavior

Search across:

- reference number
- title
- developer
- consultant
- lot number
- mukim
- planning block

## 16.6 Feature detail drawer

Display:

- layer type
- file/reference number
- public title
- current status
- lot number
- mukim
- planning block
- developer
- consultant
- application year
- approval year
- site area

Do not display:

- unreviewed owner fields by default

## 16.7 Empty and loading states

Required states:

- no features at current zoom
- no search results
- filter returns zero matches
- source temporarily unavailable

## 17. MBPJ Expansion Plan

MBPJ should be the second municipality.

## 17.1 Data acquisition approach

Since MBPJ public data is currently easier to access as page content than as a documented export:

- ingest the approved-project table from the public SmartDev page
- capture project reference and full project title text
- optionally enrich with GIS land-use context

## 17.2 Proposed acquisition method

Use a scripted scraper with:

- `httpx` or `requests`
- `BeautifulSoup` or `pandas.read_html`

If the site becomes JS-dependent:

- use Playwright

## 17.3 MBPJ challenges

- likely no clean polygon export on the first pass
- data is probably project-list text before it is geospatial data
- location parsing may be noisy

## 17.4 MBPJ strategy

Do not block MVP on MBPJ.

Instead:

- treat MBPJ as a text-first project register
- later geocode or lot-match where possible
- use MBPJ GIS layers for context

## 17.5 MBPJ implementation objective for the next phase

The next phase should not try to force MBPJ into the same geometry-first path as MBJB on day one.

The immediate product goal for MBPJ is:

- acquire a stable public project register
- normalize it into the same canonical application model where possible
- attach MBPJ municipality metadata and public-safe search fields
- preserve raw source snapshots and run manifests exactly as with MBJB
- publish MBPJ first as a searchable municipality dataset
- only promote MBPJ onto the public map once geometry quality is acceptable

## 17.6 Recommended MBPJ build sequence

Execute the MBPJ phase in this order:

1. verify the current public MBPJ SmartDev source shape and capture raw HTML snapshots
2. confirm whether MBPJ exposes any public GIS or downloadable geometry that can be linked safely
3. create a dedicated `source_system` such as `mbpj_smartdev`
4. ingest the public project register into `raw` and `stage`
5. normalize:
   - reference number
   - project title
   - status
   - locality text
   - municipality
   - available dates
6. create an MBPJ-specific public-safe title and search row
7. if no trusted geometry exists yet:
   - expose MBPJ in search and analytics first
   - keep geometry nullable
   - do not fake polygons
8. if trusted geometry becomes available:
   - add MBPJ publish tables and tiles using the same `core` and `marts` pattern as MBJB

## 17.7 MBPJ acceptance criteria for phase 1

The first MBPJ phase is successful when:

- raw MBPJ source snapshots are preserved per run
- an MBPJ normalized dataset can be loaded repeatably
- search works on known MBPJ project references or titles
- public-safe fields are reviewed before exposure
- geometry handling is explicit:
  - either trusted map geometry is available and documented
  - or MBPJ remains text-first until a valid geometry source is secured

## 18. Optional Building Footprint Integration

This is **not** required for MVP because MBJB already has development polygons.

Add footprints later if we want:

- more building-like 3D rendering
- fallback support for other municipalities that only expose point or text records
- richer spatial context

## 18.1 Candidate footprint sources

- Microsoft Global ML Building Footprints
- Google Open Buildings
- OpenStreetMap / Geofabrik

## 18.2 How footprints would be used later

- join permit polygons to existing building footprints by overlap
- where a permit polygon is very large, optionally derive affected structures
- where a municipality provides a point only, snap to likely building polygons

## 19. Data Quality and QA

This project lives or dies on data quality.

## 19.1 Required QA checks per run

- record count by source layer
- duplicate object ID detection
- duplicate reference number detection
- null rate checks on key fields
- invalid geometry count
- zero-area geometry count
- out-of-bound geometry detection
- major schema drift detection

## 19.2 Regression baseline

Create baseline counts from the first successful MBJB run:

- `PelanBangunan`: `618`
- `KebenaranMerancang`: `310`
- `KerjaTanah`: `190`

If future runs deviate materially, flag:

- more than 10 percent count drop
- required field removed
- geometry extent shift outside expected municipal extent

## 19.3 Manual QA workflow

For each major release:

- sample 20 polygons from each layer
- verify metadata fields are populated sensibly
- verify polygon placement against basemap
- verify status filter and year filter counts
- verify search by known file number

## 20. Security, Privacy, and Legal Controls

## 20.1 Data minimization for public UI

Do not expose every ingested field by default.

Create a field allowlist for public output:

- `reference_no`
- `title`
- `layer_type`
- `status`
- `year`
- `lot_no`
- `mukim`
- `planning_block`
- `developer_name`
- `consultant_name`
- `site_area`

Keep internal-only unless reviewed:

- `owner_name_raw`
- operational notes
- raw decision memo text if exposed later

## 20.2 Rate limiting and access etiquette

- cache source downloads
- do not hammer public services
- use polite batch sizes
- schedule off-peak refreshes where possible

## 20.3 Attribution

The app should include a data sources page with:

- source portal names
- dates ingested
- attribution statements
- links to official portals

## 21. Observability and Operations

## 21.1 Logging

Each ingest run should log:

- source name
- start and end time
- request counts
- failures and retries
- records ingested
- records normalized
- records published

## 21.2 Run artifacts

Persist:

- run manifest JSON
- counts by layer
- schema snapshot
- QA results

## 21.3 Failure handling

If a run fails:

- preserve successful prior publish tables
- mark the run failed in `meta.ingest_runs`
- send operator-visible error output

## 22. Local Development Environment

## 22.1 Recommended Docker Compose services

- `db` -> Postgres/PostGIS
- `tiles` -> pg_tileserv
- `web` -> Next.js app

Optional:

- `adminer` or `pgadmin`
- `metabase`

## 22.2 Local workflow

1. start database and tile server
2. run the MBJB pipeline script
3. load normalized data to PostGIS
4. boot frontend
5. verify tiles and filters

## 22.3 Environment variables

Suggested `.env.example`:

```env
WEB_PORT=3001
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/permits
NEXT_PUBLIC_MAP_STYLE_URL=https://demotiles.maplibre.org/style.json
PGTILESERV_DATABASE_URL=postgresql://postgres:postgres@db:5432/permits
APP_ENV=development
```

## 23. Milestone Plan

Milestones `1` through `6` are effectively complete for the MBJB MVP as of March 6, 2026.

## Milestone 1: Project bootstrap (`completed`)

Deliverables:

- repository scaffold
- docker compose
- database migrations
- source registry

Acceptance:

- local services start cleanly
- migrations apply successfully

## Milestone 2: MBJB ingestion (`completed`)

Deliverables:

- ArcGIS REST ingestion script
- raw snapshot storage
- normalized GeoParquet outputs

Acceptance:

- all 3 target layers download successfully
- run manifest records observed counts

## Milestone 3: Warehouse and publish model (`completed`)

Deliverables:

- PostGIS stage and core tables
- canonical field mapping
- QA checks
- materialized stats views

Acceptance:

- queried records match normalized outputs
- geometry indexes are in place

## Milestone 4: Tile server and APIs (`completed`)

Deliverables:

- pg_tileserv config
- app API endpoints
- search endpoint

Acceptance:

- tiles render in a browser
- stats endpoint returns counts

## Milestone 5: Frontend MVP (`completed`)

Deliverables:

- main map
- layer toggles
- filters
- search
- detail drawer
- source attribution page

Acceptance:

- user can find and inspect a known MBJB development polygon end to end

## Milestone 6: QA and release hardening (`completed`)

Deliverables:

- data QA report
- performance review
- privacy review of public fields
- operator runbook

Acceptance:

- no critical UI or data issues
- deployment repeatable from docs

## Milestone 7: MBPJ source acquisition and text-first normalization (`next`)

Deliverables:

- MBPJ source verification notes
- immutable raw MBPJ snapshots
- MBPJ normalization script
- MBPJ canonical stage output
- MBPJ search-ready public-safe dataset

Acceptance:

- MBPJ ingestion is repeatable
- a known MBPJ project can be found in the local app or API layer
- the plan clearly records whether MBPJ geometry is trustworthy enough for public map rendering

## 24. Suggested Timeline

Aggressive but realistic MVP timeline:

### Week 1

- repo bootstrap
- database setup
- MBJB ingest prototype
- raw and stage artifacts

### Week 2

- canonical model
- PostGIS load
- tiles
- basic stats

### Week 3

- frontend map
- filters
- search
- detail drawer

### Week 4

- QA
- privacy review
- production deployment
- documentation

## 25. Acceptance Criteria for Final MVP

The MVP is done when all of the following are true:

- MBJB data ingestion is repeatable and automated
- all 3 MBJB development layers are available in PostGIS
- the frontend can render those layers from tiles
- a user can filter by layer, status, and year
- a user can search by reference number or project text
- the detail drawer shows a reviewed metadata allowlist
- zoning or administrative context can be toggled on
- data source attribution is visible in the app
- the system can be redeployed from documentation

## 26. Risks and Mitigations

## Risk 1: source schema drift

Mitigation:

- snapshot metadata every run
- fail loudly if required fields disappear

## Risk 2: public access does not imply public republication rights

Mitigation:

- maintain internal raw store
- expose reviewed fields only
- verify municipal terms before public launch

## Risk 3: performance degrades as municipalities are added

Mitigation:

- tiles from day one
- PostGIS indexes
- precomputed marts for dashboards

## Risk 4: source outages

Mitigation:

- do not depend on live source availability at runtime
- ingest on schedule and serve from our own warehouse

## Risk 5: sensitive fields appear in titles or raw attributes

Mitigation:

- create public-safe derived fields
- manual review before launch

## 27. Post-MVP Backlog

After the first MBJB release:

- add MBPJ approved-project ingestion and source QA
- add MBPJ municipality support in the UI once geometry and licensing are acceptable
- add Penang open data overlays
- add MBSP zoning overlays
- add change tracking between ingest runs
- add download exports for public-safe datasets
- add timeline playback
- add footprint integration for richer 3D representation
- add municipality switcher
- add operator dashboard for ingest health

## 28. Immediate Next Build Steps

MBJB implementation is complete enough that the next build steps should now focus on `MBPJ`.

Execute in this order:

1. verify the current MBPJ public source shape and access pattern
2. document MBPJ licensing and republication constraints before exposing data publicly
3. add MBPJ raw snapshot capture and run manifests
4. normalize MBPJ project-register rows into the canonical application schema where fields exist
5. create MBPJ search and stats outputs even if geometry is still null
6. evaluate whether MBPJ GIS geometry can be linked safely and accurately
7. only after that, decide whether MBPJ should ship as:
   - a text-first searchable municipality
   - or a full second mapped municipality

## 29. Recommended Decision

Do **not** start this as a generic nationwide permit map.

Start it as:

- a **Johor Bahru development map**
- built on top of **MBJB GeoJB**
- with architecture designed to expand to MBPJ, Penang, MBSP, Selangor, and DBKL later

That gives the project:

- real public data
- polygon geometry from day one
- a tractable MVP
- a clean path to a broader Malaysian municipal permits platform

## 30. Key Source URLs

### MBJB

- GeoJB portal: `https://geojb.gov.my/gisportal/apps/sites/#/mbjb-geojb`
- services root: `https://geojb.gov.my/gisserver/rest/services?f=pjson`
- GEOJB services: `https://geojb.gov.my/gisserver/rest/services/GEOJB?f=pjson`
- MaklumatPembangunan service: `https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson`
- KebenaranMerancang layer: `https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer/3?f=pjson`

### MBPJ

- SmartDev home: `https://mbpjsmartdev.pjsmartcity.gov.my/`
- project feedback lookup: `https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas`
- GIS portal: `http://pjcityplan.mbpj.gov.my:81/mapguide/mbpjfusion/main.php`

### Penang

- Penang GIS Open Data: `https://data-pegis.opendata.arcgis.com/`
- Penang state portal: `https://www.penang.gov.my/`

### MBSP

- town planning page: `https://www.mbsp.gov.my/index.php/en/form/town-planning`
- SPGIS portal: `https://spgis.mbsp.gov.my/portal/home/`

### Selangor

- SISMAPS info page: `https://www.jpbdselangor.gov.my/index.php/en/perkhidmatan/sistem-maklumat-geografi-gis/sistem-maklumat-perancangan-negeri-selangor-sismaps`
- SISMAPS portal: `https://sismaps.jpbdselangor.gov.my/index`

### Kuala Lumpur

- CPS portal: `https://cps.dbkl.gov.my/`
- DBKL CPS page: `https://www.dbkl.gov.my/en/online-services/city-planning-system-cps`

### National / support

- data.gov.my archive root: `https://archive.data.gov.my/`
- Microsoft building footprints: `https://github.com/microsoft/GlobalMLBuildingFootprints`
- Google Open Buildings: `https://sites.research.google/open-buildings/`
- Geofabrik Malaysia extract: `https://download.geofabrik.de/asia/malaysia-singapore-brunei.html`
