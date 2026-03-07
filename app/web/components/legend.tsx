import type { MunicipalityCode } from "@/lib/types";

type Props = {
  municipality: MunicipalityCode;
};

export function Legend({ municipality }: Props) {
  const items =
    municipality === "MBPJ"
      ? [{ color: "rgb(170,108,56)", label: "MBPJ official buildings context" }]
      : [
          { color: "rgb(41,109,84)", label: "Kebenaran Merancang" },
          { color: "rgb(34,87,155)", label: "Pelan Bangunan" },
          { color: "rgb(194,110,38)", label: "Kerja Tanah" }
        ];

  return (
    <section className="detail-card">
      <p className="eyebrow">Legend</p>
      <p className="muted">
        {municipality === "MBPJ"
          ? "MBPJ shows official-building context polygons plus the municipal boundary. SmartDev project rows remain searchable text records unless a direct project geometry source is found."
          : "Green, blue, and orange are the development polygons. Planning block dots are neutral context markers."}
      </p>
      <div className="legend-list">
        {items.map((item) => (
          <div key={item.label} className="legend-item">
            <span className="swatch" style={{ background: item.color }} />
            <span>{item.label}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
