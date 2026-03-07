export default function SourcesPage() {
  return (
    <main className="sources-page">
      <div className="sources-card">
        <p className="eyebrow">Attribution</p>
        <h1>Source and Licensing Notes</h1>
        <p>
          The current local stack uses public MBJB GeoJB ArcGIS REST services for mapped polygon
          data, plus MBPJ SmartDev for approved-project text records and MBPJ ArcGIS for context geometry.
        </p>
        <h2>MBJB map-enabled source</h2>
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
        <h2>MBPJ sources</h2>
        <ul>
          <li>
            SmartDev homepage:
            <a href="https://mbpjsmartdev.pjsmartcity.gov.my/">
              https://mbpjsmartdev.pjsmartcity.gov.my/
            </a>
          </li>
          <li>
            Feedback lookup:
            <a href="https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas">
              https://mbpjsmartdev.pjsmartcity.gov.my/sessions/semakmaklumbalas
            </a>
          </li>
          <li>
            ArcGIS FeatureServer used for context geometry:
            <a href="https://services3.arcgis.com/Pm8D5pANQ4gpdLsD/arcgis/rest/services/MBPJ/FeatureServer?f=pjson">
              https://services3.arcgis.com/Pm8D5pANQ4gpdLsD/arcgis/rest/services/MBPJ/FeatureServer
            </a>
          </li>
        </ul>
        <p>
          Public accessibility does not automatically imply an open license for unrestricted
          republication. Raw source snapshots are kept privately on disk. The UI exposes a reviewed
          public field allowlist and intentionally excludes raw owner fields such as <code>PEMILIK</code>.
        </p>
        <p>
          MBPJ is now rendered with context geometry only: official-building polygons and the
          municipality boundary are shown on the map, while SmartDev approved-project rows stay
          geometry-null because no direct public project polygons were matched to them.
        </p>
      </div>
    </main>
  );
}
