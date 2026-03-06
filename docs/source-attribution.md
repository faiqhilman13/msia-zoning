# Source Attribution And Licensing Notes

## Primary source

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
