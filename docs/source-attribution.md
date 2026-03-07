# Source Attribution And Licensing Notes

## MBJB map-enabled source

This MVP uses public MBJB GeoJB ArcGIS REST services:

- GeoJB portal: `https://geojb.gov.my/gisportal/apps/sites/#/mbjb-geojb`
- services root: `https://geojb.gov.my/gisserver/rest/services/GEOJB?f=pjson`
- MaklumatPembangunan service: `https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson`

## Development layers used

- `PelanBangunan` layer `1`
- `KebenaranMerancang` layer `3`
- `KerjaTanah` layer `4`

## Context layers used

- `Sempadan_MBJB_Merge` layer `1`
- `RancanganTempatan` layer `0`
- `Mukim_161025` layer `0`

## Preserved source facts

- geometry contract: polygon data
- supported export formats observed in the public layer metadata include:
  - `geojson`
  - `csv`
  - `shapefile`
- observed public baseline counts on March 6, 2026:
  - `PelanBangunan`: `618`
  - `KebenaranMerancang`: `310`
  - `KerjaTanah`: `190`

## Attribution guidance

- Attribute MBJB GeoJB as the upstream source for the development and context layers.
- Treat the official MBJB GeoJB portal and services as the authoritative source of record.
- Make clear that this project republishes a reviewed subset of public fields for usability and transparency.

## Licensing and redistribution caution

Public accessibility does **not** automatically imply an open license for unrestricted republication.

Current project posture:

- keep raw source snapshots internal
- expose only reviewed fields in the public app
- avoid publishing sensitive/raw owner fields by default
- include clear source attribution
- verify MBJB terms and any downstream redistribution constraints before any broader public launch

## MBPJ text-first source

The MBPJ phase uses public SmartDev HTML pages as the current project-register source:

- homepage: `https://mbpjsmartdev.pjsmartcity.gov.my/`
- feedback lookup: `https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas`
- terms: `https://mbpjsmartdev.pjsmartcity.gov.my/pages/terma-_-syarat`
- disclaimer: `https://mbpjsmartdev.pjsmartcity.gov.my/pages/penafian`
- GIS landing page reviewed for geometry verification:
  - `http://pjcityplan.mbpj.gov.my:81/mapguide/mbpjfusion/public_authhenticate.php`

## MBPJ source facts preserved in code and raw snapshots

- the SmartDev homepage currently embeds a static HTML table titled `Senarai Projek Yang Diluluskan`
- the observed table shape is:
  - `Bil.`
  - `Tajuk Projek`
- the project reference number appears inside each row as a highlighted block when present
- observed homepage count on March 6, 2026: `62` approved-project rows
- several rows do not currently expose a populated reference number block, so the pipeline falls back to a deterministic title-based natural key

## MBPJ licensing and geometry posture

- SmartDev pages show `Hakcipta Terpelihara 2026 © Jabatan Perancangan Pembangunan, MBPJ`
- the terms and disclaimer pages do not grant an explicit open-data redistribution license
- the public GIS landing page is suitable for human review only at this stage
- no trustworthy public downloadable MBPJ geometry source was verified for public map rendering during this phase

Current project posture for MBPJ:

- preserve raw HTML snapshots privately on disk per run
- normalize MBPJ into the canonical application model where fields exist
- store extracted party text internally as `owner_name_raw`
- do not expose owner-like fields publicly by default
- treat MBPJ as `text_first` until a trustworthy public geometry source and redistribution posture are confirmed
