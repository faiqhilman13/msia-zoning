export function Legend() {
  return (
    <section className="detail-card">
      <p className="eyebrow">Legend</p>
      <p className="muted">Green, blue, and orange are the development polygons. Planning block dots are neutral context markers.</p>
      <div className="legend-list">
        <div className="legend-item">
          <span className="swatch" style={{ background: "rgb(41,109,84)" }} />
          <span>Kebenaran Merancang</span>
        </div>
        <div className="legend-item">
          <span className="swatch" style={{ background: "rgb(34,87,155)" }} />
          <span>Pelan Bangunan</span>
        </div>
        <div className="legend-item">
          <span className="swatch" style={{ background: "rgb(194,110,38)" }} />
          <span>Kerja Tanah</span>
        </div>
      </div>
    </section>
  );
}
