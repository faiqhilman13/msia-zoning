# Data Dictionary

## Canonical identifiers

- `source_system` - source namespace, currently `mbjb_geojb` or `mbpj_smartdev`
- `source_layer` - source layer slug such as `kebenaran_merancang`
- `source_object_id` - stable source record identifier (ArcGIS object ID for MBJB, deterministic natural-key hash for MBPJ)
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
| `area_m2` | polygon area in square meters when geometry exists |
| `area_acres` | polygon area in acres when geometry exists |
| `centroid` | representative point for search and map interactions when geometry exists |
| `geometry` | source polygon geometry, normalized to `MultiPolygon` when available |

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

Cross-municipality public-safe records are exposed through `marts.public_applications`.

The current map-enabled tile view still uses `marts.mbjb_public_features`.

`marts.public_applications` includes both geometry-backed and text-first rows:

- `has_geometry = true` for MBJB map-enabled records
- `has_geometry = false` for MBPJ text-first records

Public context overlays use:

- `marts.mbjb_context_planning_blocks`
- `marts.mbjb_context_boundaries`
