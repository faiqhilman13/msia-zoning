CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS meta.source_registry (
    source_system text PRIMARY KEY,
    source_name text NOT NULL,
    source_url text NOT NULL,
    municipality_code text NOT NULL,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.ingest_runs (
    ingest_run_id uuid PRIMARY KEY,
    source_system text NOT NULL REFERENCES meta.source_registry(source_system),
    municipality_code text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    run_status text NOT NULL,
    run_label text,
    observed_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    raw_root text NOT NULL,
    stage_root text NOT NULL,
    qa_report_path text,
    notes text
);

CREATE TABLE IF NOT EXISTS meta.ingest_artifacts (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    artifact_type text NOT NULL,
    relative_path text NOT NULL,
    file_size_bytes bigint,
    sha256 text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, artifact_type, relative_path)
);

CREATE TABLE IF NOT EXISTS meta.quality_checks (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    check_name text NOT NULL,
    check_status text NOT NULL,
    observed_value jsonb,
    details text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, check_name)
);

CREATE TABLE IF NOT EXISTS raw.mbjb_kebenaran_merancang (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_layer text NOT NULL DEFAULT 'kebenaran_merancang',
    reference_no text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    geometry geometry(MultiPolygon, 4326),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS raw.mbjb_pelan_bangunan (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_layer text NOT NULL DEFAULT 'pelan_bangunan',
    reference_no text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    geometry geometry(MultiPolygon, 4326),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS raw.mbjb_kerja_tanah (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_layer text NOT NULL DEFAULT 'kerja_tanah',
    reference_no text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    geometry geometry(MultiPolygon, 4326),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS raw.mbpj_project_register (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_row_number integer NOT NULL,
    reference_no text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS raw.mbpj_official_buildings (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_layer text NOT NULL DEFAULT 'official_buildings',
    name text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    geometry geometry(MultiPolygon, 4326),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS raw.mbpj_boundary (
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_object_id bigint NOT NULL,
    source_layer text NOT NULL DEFAULT 'municipality_boundary',
    name text,
    raw_attributes jsonb NOT NULL,
    raw_record_hash text NOT NULL,
    geometry geometry(MultiPolygon, 4326),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ingest_run_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS stage.mbjb_development_unified (
    application_id uuid PRIMARY KEY,
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_system text NOT NULL,
    source_municipality text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    reference_no text,
    reference_no_alt text,
    title text,
    application_type text NOT NULL,
    current_status text,
    status_raw text,
    application_year integer,
    approval_year integer,
    meeting_date_1 timestamptz,
    meeting_decision_1 text,
    meeting_date_2 timestamptz,
    meeting_decision_2 text,
    meeting_date_3 timestamptz,
    meeting_decision_3 text,
    meeting_date_4 timestamptz,
    meeting_decision_4 text,
    lot_no text,
    mukim text,
    planning_block text,
    zoning_name text,
    owner_name_raw text,
    developer_name text,
    consultant_name text,
    proxy_holder_name text,
    site_area_acres numeric,
    site_area_m2 numeric,
    area_m2 numeric NOT NULL,
    area_acres numeric NOT NULL,
    public_display_title text NOT NULL,
    public_display_status text NOT NULL,
    is_public_visible boolean NOT NULL DEFAULT true,
    raw_record_hash text NOT NULL,
    raw_attributes jsonb NOT NULL,
    centroid geometry(Point, 4326) NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS stage.mbpj_project_register (
    application_id uuid PRIMARY KEY,
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_system text NOT NULL,
    source_municipality text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    source_row_number integer NOT NULL,
    source_url text NOT NULL,
    reference_no text,
    reference_no_alt text,
    title text,
    application_type text NOT NULL,
    current_status text,
    status_raw text,
    application_year integer,
    approval_year integer,
    meeting_date_1 timestamptz,
    meeting_decision_1 text,
    meeting_date_2 timestamptz,
    meeting_decision_2 text,
    meeting_date_3 timestamptz,
    meeting_decision_3 text,
    meeting_date_4 timestamptz,
    meeting_decision_4 text,
    lot_no text,
    mukim text,
    planning_block text,
    zoning_name text,
    owner_name_raw text,
    developer_name text,
    consultant_name text,
    proxy_holder_name text,
    site_area_acres numeric,
    site_area_m2 numeric,
    area_m2 numeric,
    area_acres numeric,
    public_display_title text NOT NULL,
    public_display_status text NOT NULL,
    is_public_visible boolean NOT NULL DEFAULT true,
    raw_record_hash text NOT NULL,
    raw_attributes jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS stage.mbpj_official_buildings (
    source_object_id bigint PRIMARY KEY,
    municipality_code text NOT NULL,
    context_type text NOT NULL,
    source_system text NOT NULL,
    source_layer text NOT NULL,
    name text,
    category text,
    address text,
    inspection_status text,
    raw_record_hash text NOT NULL,
    raw_attributes jsonb NOT NULL,
    centroid geometry(Point, 4326) NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS stage.mbpj_boundary (
    source_object_id bigint PRIMARY KEY,
    municipality_code text NOT NULL,
    context_type text NOT NULL,
    source_system text NOT NULL,
    source_layer text NOT NULL,
    name text,
    category text,
    raw_record_hash text NOT NULL,
    raw_attributes jsonb NOT NULL,
    centroid geometry(Point, 4326) NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS core.development_applications (
    application_id uuid PRIMARY KEY,
    source_system text NOT NULL,
    source_municipality text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    reference_no text,
    reference_no_alt text,
    title text,
    application_type text NOT NULL,
    current_status text,
    status_raw text,
    application_year integer,
    approval_year integer,
    meeting_date_1 timestamptz,
    meeting_decision_1 text,
    meeting_date_2 timestamptz,
    meeting_decision_2 text,
    meeting_date_3 timestamptz,
    meeting_decision_3 text,
    meeting_date_4 timestamptz,
    meeting_decision_4 text,
    lot_no text,
    mukim text,
    planning_block text,
    zoning_name text,
    owner_name_raw text,
    developer_name text,
    consultant_name text,
    proxy_holder_name text,
    site_area_acres numeric,
    site_area_m2 numeric,
    public_display_title text NOT NULL,
    public_display_status text NOT NULL,
    is_public_visible boolean NOT NULL DEFAULT true,
    raw_record_hash text NOT NULL,
    ingest_run_id uuid NOT NULL REFERENCES meta.ingest_runs(ingest_run_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS core.development_geometries (
    geometry_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id uuid NOT NULL REFERENCES core.development_applications(application_id) ON DELETE CASCADE,
    geometry geometry(MultiPolygon, 4326) NOT NULL,
    centroid geometry(Point, 4326) NOT NULL,
    bbox geometry(Polygon, 4326) GENERATED ALWAYS AS (ST_Envelope(geometry)) STORED,
    geometry_type text NOT NULL DEFAULT 'MultiPolygon',
    area_m2 numeric NOT NULL,
    area_acres numeric NOT NULL,
    is_valid_geometry boolean NOT NULL,
    geometry_source text NOT NULL DEFAULT 'mbjb_geojb',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (application_id)
);

CREATE TABLE IF NOT EXISTS core.zoning_areas (
    zoning_area_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    municipality_code text NOT NULL,
    zoning_type text NOT NULL,
    zoning_code text,
    zoning_name text,
    mukim text,
    source_system text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS core.admin_boundaries (
    boundary_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    municipality_code text NOT NULL,
    boundary_type text NOT NULL,
    boundary_code text,
    boundary_name text NOT NULL,
    source_system text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS core.context_features (
    context_feature_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    municipality_code text NOT NULL,
    context_type text NOT NULL,
    name text,
    category text,
    address text,
    inspection_status text,
    source_system text NOT NULL,
    source_layer text NOT NULL,
    source_object_id bigint NOT NULL,
    raw_record_hash text NOT NULL,
    raw_attributes jsonb NOT NULL,
    centroid geometry(Point, 4326) NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_system, source_layer, source_object_id)
);

CREATE TABLE IF NOT EXISTS core.search_documents (
    search_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id uuid NOT NULL REFERENCES core.development_applications(application_id) ON DELETE CASCADE,
    reference_no text,
    title text,
    developer_name text,
    consultant_name text,
    lot_no text,
    mukim text,
    planning_block text,
    search_tsv tsvector NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (application_id)
);

DROP VIEW IF EXISTS marts.stats_by_year;
DROP VIEW IF EXISTS marts.stats_by_status;
DROP VIEW IF EXISTS marts.stats_by_layer;
DROP VIEW IF EXISTS marts.stats_overview;
DROP VIEW IF EXISTS marts.mbpj_context_boundaries;
DROP VIEW IF EXISTS marts.mbpj_context_buildings;
DROP VIEW IF EXISTS marts.mbjb_context_boundaries;
DROP VIEW IF EXISTS marts.mbjb_context_planning_blocks;
DROP VIEW IF EXISTS marts.mbjb_public_features;
DROP VIEW IF EXISTS marts.public_applications;

CREATE OR REPLACE VIEW marts.public_applications AS
SELECT
    a.application_id,
    a.source_system,
    a.source_municipality,
    a.source_layer AS layer_type,
    a.application_type,
    a.reference_no,
    a.reference_no_alt,
    a.public_display_title,
    a.public_display_status,
    a.application_year,
    a.approval_year,
    a.lot_no,
    a.mukim,
    a.planning_block,
    a.zoning_name,
    a.developer_name,
    a.consultant_name,
    CASE WHEN g.application_id IS NOT NULL THEN true ELSE false END AS has_geometry,
    g.area_m2,
    g.area_acres,
    ST_X(g.centroid) AS centroid_lon,
    ST_Y(g.centroid) AS centroid_lat,
    g.centroid,
    g.geometry
FROM core.development_applications a
LEFT JOIN core.development_geometries g USING (application_id)
WHERE a.is_public_visible = true;

CREATE OR REPLACE VIEW marts.mbjb_public_features AS
SELECT
    application_id,
    source_municipality,
    layer_type,
    application_type,
    reference_no,
    reference_no_alt,
    public_display_title,
    public_display_status,
    application_year,
    approval_year,
    lot_no,
    mukim,
    planning_block,
    zoning_name,
    developer_name,
    consultant_name,
    area_m2,
    area_acres,
    centroid_lon,
    centroid_lat,
    geometry
FROM marts.public_applications
WHERE source_municipality = 'MBJB'
  AND has_geometry = true;

CREATE OR REPLACE VIEW marts.mbjb_context_planning_blocks AS
SELECT
    zoning_area_id,
    zoning_code AS planning_block,
    zoning_name,
    mukim,
    geometry
FROM core.zoning_areas
WHERE municipality_code = 'MBJB'
  AND zoning_type = 'planning_block';

CREATE OR REPLACE VIEW marts.mbjb_context_boundaries AS
SELECT
    boundary_id,
    boundary_type,
    boundary_name,
    geometry
FROM core.admin_boundaries
WHERE municipality_code = 'MBJB';

CREATE OR REPLACE VIEW marts.mbpj_context_buildings AS
SELECT
    context_feature_id,
    municipality_code,
    context_type,
    name,
    category,
    address,
    inspection_status,
    source_layer,
    source_object_id,
    ST_X(centroid) AS centroid_lon,
    ST_Y(centroid) AS centroid_lat,
    geometry
FROM core.context_features
WHERE municipality_code = 'MBPJ'
  AND context_type = 'official_building';

CREATE OR REPLACE VIEW marts.mbpj_context_boundaries AS
SELECT
    boundary_id,
    boundary_type,
    boundary_name,
    geometry
FROM core.admin_boundaries
WHERE municipality_code = 'MBPJ';

CREATE OR REPLACE VIEW marts.stats_overview AS
SELECT
    source_municipality AS municipality_code,
    count(*) AS total_features,
    count(*) FILTER (WHERE has_geometry) AS map_enabled_features,
    count(*) FILTER (WHERE NOT has_geometry) AS text_only_features,
    count(*) FILTER (WHERE public_display_status = 'approved') AS approved_features,
    count(*) FILTER (WHERE public_display_status = 'pending') AS pending_features,
    round(sum(area_acres)::numeric, 2) AS total_area_acres
FROM marts.public_applications
GROUP BY 1;

CREATE OR REPLACE VIEW marts.stats_by_layer AS
SELECT layer_type, count(*) AS feature_count
FROM marts.public_applications
GROUP BY 1
ORDER BY 2 DESC, 1;

CREATE OR REPLACE VIEW marts.stats_by_status AS
SELECT public_display_status, count(*) AS feature_count
FROM marts.public_applications
GROUP BY 1
ORDER BY 2 DESC, 1;

CREATE OR REPLACE VIEW marts.stats_by_year AS
SELECT COALESCE(approval_year, application_year) AS year, count(*) AS feature_count
FROM marts.public_applications
GROUP BY 1
ORDER BY 1 NULLS LAST;

CREATE INDEX IF NOT EXISTS idx_stage_mbjb_dev_geom ON stage.mbjb_development_unified USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_stage_mbjb_dev_centroid ON stage.mbjb_development_unified USING gist (centroid);
CREATE INDEX IF NOT EXISTS idx_core_app_layer ON core.development_applications(source_layer);
CREATE INDEX IF NOT EXISTS idx_core_app_municipality ON core.development_applications(source_municipality);
CREATE INDEX IF NOT EXISTS idx_core_app_status ON core.development_applications(public_display_status);
CREATE INDEX IF NOT EXISTS idx_core_app_years ON core.development_applications(application_year, approval_year);
CREATE INDEX IF NOT EXISTS idx_core_app_reference ON core.development_applications(reference_no);
CREATE INDEX IF NOT EXISTS idx_core_app_mukim ON core.development_applications(mukim);
CREATE INDEX IF NOT EXISTS idx_core_app_planning_block ON core.development_applications(planning_block);
CREATE INDEX IF NOT EXISTS idx_core_app_developer ON core.development_applications(developer_name);
CREATE INDEX IF NOT EXISTS idx_core_app_consultant ON core.development_applications(consultant_name);
CREATE INDEX IF NOT EXISTS idx_core_geom_geom ON core.development_geometries USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_core_geom_centroid ON core.development_geometries USING gist (centroid);
CREATE INDEX IF NOT EXISTS idx_core_zoning_geom ON core.zoning_areas USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_core_admin_geom ON core.admin_boundaries USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_core_context_geom ON core.context_features USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_core_context_centroid ON core.context_features USING gist (centroid);
CREATE INDEX IF NOT EXISTS idx_core_context_municipality ON core.context_features(municipality_code, context_type);
CREATE INDEX IF NOT EXISTS idx_core_search_tsv ON core.search_documents USING gin (search_tsv);

INSERT INTO meta.source_registry (source_system, source_name, source_url, municipality_code, notes)
VALUES (
    'mbjb_geojb',
    'MBJB GeoJB',
    'https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson',
    'MBJB',
    'Public ArcGIS REST services for MBJB development layers and context overlays.'
)
ON CONFLICT (source_system) DO UPDATE
SET
    source_name = EXCLUDED.source_name,
    source_url = EXCLUDED.source_url,
    municipality_code = EXCLUDED.municipality_code,
    notes = EXCLUDED.notes;

INSERT INTO meta.source_registry (source_system, source_name, source_url, municipality_code, notes)
VALUES (
    'mbpj_smartdev',
    'MBPJ SmartDev',
    'https://mbpjsmartdev.pjsmartcity.gov.my/',
    'MBPJ',
    'Public MBPJ SmartDev approved-project register captured as HTML. Project rows remain text-first even when MBPJ context geometry is available separately.'
)
ON CONFLICT (source_system) DO UPDATE
SET
    source_name = EXCLUDED.source_name,
    source_url = EXCLUDED.source_url,
    municipality_code = EXCLUDED.municipality_code,
    notes = EXCLUDED.notes;

INSERT INTO meta.source_registry (source_system, source_name, source_url, municipality_code, notes)
VALUES (
    'mbpj_arcgis',
    'MBPJ ArcGIS',
    'https://services3.arcgis.com/Pm8D5pANQ4gpdLsD/arcgis/rest/services/MBPJ/FeatureServer?f=pjson',
    'MBPJ',
    'Public MBPJ ArcGIS context geometry for official buildings and municipality boundary. Not a direct project-polygon source.'
)
ON CONFLICT (source_system) DO UPDATE
SET
    source_name = EXCLUDED.source_name,
    source_url = EXCLUDED.source_url,
    municipality_code = EXCLUDED.municipality_code,
    notes = EXCLUDED.notes;
