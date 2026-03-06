# Data Dictionary

## Canonical identifiers

- `source_system` - source namespace, currently `mbjb_geojb`
- `source_layer` - source layer slug such as `kebenaran_merancang`
- `source_object_id` - ArcGIS object ID
- `application_id` - deterministic UUID5 derived from the natural key

## Core public fields

| Field | Meaning |
| --- | --- |
| `reference_no` | primary file/reference number |
| `reference_no_alt` | alternate reference number when available |
| `public_display_title` | safe public-facing title |
| `public_display_status` | normalized public status bucket |
| `application_type` | MBJB application type label |
| `application_year` | application year |
| `approval_year` | approval year |
| `lot_no` | lot identifier when available |
| `mukim` | canonical mukim label |
| `planning_block` | canonical planning block label |
| `zoning_name` | planning block/zoning name from context layers when available |
| `developer_name` | developer name when available |
| `consultant_name` | consultant name when available |
| `area_m2` | polygon area in square meters |
| `area_acres` | polygon area in acres |
| `centroid` | representative point for search and map interactions |
| `geometry` | source polygon geometry, normalized to `MultiPolygon` |

## Internal-only raw fields

These are retained for lineage and operational QA but are not intended for public display by default:

- `owner_name_raw`
- `proxy_holder_name`
- `raw_attributes`
- `raw_record_hash`
- raw tables under `raw.*`

## Database layers

- `meta.*` - run registry, artifact manifests, QA records
- `raw.*` - per-layer raw source capture
- `stage.*` - normalized staging tables mirroring the latest transformation
- `core.*` - durable application and geometry tables
- `marts.*` - public-serving and analytics views

## Public serving view

The frontend and tile server use `marts.mbjb_public_features`.

Public context overlays use:

- `marts.mbjb_context_planning_blocks`
- `marts.mbjb_context_boundaries`
