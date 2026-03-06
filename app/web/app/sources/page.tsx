export default function SourcesPage() {
  return (
    <main className="sources-page">
      <div className="sources-card">
        <p className="eyebrow">Attribution</p>
        <h1>Source and Licensing Notes</h1>
        <p>
          This MVP uses public MBJB GeoJB ArcGIS REST services for development polygons and
          supporting planning context. The current MBJB development service exposes polygon
          geometry and export formats including GeoJSON, CSV, shapefile, and file geodatabase.
        </p>
        <ul>
          <li>
            Maklumat Pembangunan FeatureServer:
            <a href="https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer?f=pjson">
              https://geojb.gov.my/gisserver/rest/services/GEOJB/MaklumatPembangunan/FeatureServer
            </a>
          </li>
          <li>
            Rancangan Tempatan FeatureServer:
            <a href="https://geojb.gov.my/gisserver/rest/services/GEOJB/RancanganTempatan/FeatureServer?f=pjson">
              https://geojb.gov.my/gisserver/rest/services/GEOJB/RancanganTempatan/FeatureServer
            </a>
          </li>
          <li>
            Sempadan MBJB FeatureServer:
            <a href="https://geojb.gov.my/gisserver/rest/services/GEOJB/Sempadan_MBJB_Merge/FeatureServer?f=pjson">
              https://geojb.gov.my/gisserver/rest/services/GEOJB/Sempadan_MBJB_Merge/FeatureServer
            </a>
          </li>
        </ul>
        <p>
          Public accessibility does not automatically imply an open license for unrestricted
          republication. Raw source snapshots are kept privately on disk. The UI exposes a reviewed
          public field allowlist and intentionally excludes raw owner fields such as <code>PEMILIK</code>.
        </p>
        <p>
          For launch beyond local/internal use, MBJB licensing terms should be reviewed explicitly.
        </p>
      </div>
    </main>
  );
}
