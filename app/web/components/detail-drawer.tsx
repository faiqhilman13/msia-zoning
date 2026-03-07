import type { FeatureDetail } from "@/lib/types";

type Props = {
  detail: FeatureDetail | null;
};

function renderValue(value: string | number | null | undefined) {
  if (value == null || value === "") {
    return "-";
  }
  return value;
}

function renderArea(acres: number | null, m2: number | null) {
  if (acres == null || m2 == null) {
    return "-";
  }
  return `${acres.toFixed(2)} ac / ${m2.toFixed(0)} m2`;
}

export function DetailDrawer({ detail }: Props) {
  const layerBreakdown =
    detail?.kind === "planning_block"
      ? detail.layerBreakdown
          .map((item) => `${item.layerType.replaceAll("_", " ")}: ${item.count}`)
          .join(" | ")
      : null;

  return (
    <section className="detail-card">
      <p className="eyebrow">Feature detail</p>
      {detail ? (
        detail.kind === "application" ? (
          <>
            <h2>{detail.title}</h2>
            <p className={`status status-${detail.status}`}>{detail.status}</p>
            <div className="detail-list">
              <div>
                <span>Municipality</span>
                <strong>{detail.municipality}</strong>
              </div>
              <div>
                <span>Reference</span>
                <strong>{renderValue(detail.referenceNo)}</strong>
              </div>
              <div>
                <span>Layer</span>
                <strong>{renderValue(detail.applicationType)}</strong>
              </div>
              <div>
                <span>Planning block</span>
                <strong>{renderValue(detail.planningBlock)}</strong>
              </div>
              <div>
                <span>Mukim</span>
                <strong>{renderValue(detail.mukim)}</strong>
              </div>
              <div>
                <span>Lot</span>
                <strong>{renderValue(detail.lotNo)}</strong>
              </div>
              <div>
                <span>Developer</span>
                <strong>{renderValue(detail.developerName)}</strong>
              </div>
              <div>
                <span>Consultant</span>
                <strong>{renderValue(detail.consultantName)}</strong>
              </div>
              <div>
                <span>Application year</span>
                <strong>{renderValue(detail.applicationYear)}</strong>
              </div>
              <div>
                <span>Approval year</span>
                <strong>{renderValue(detail.approvalYear)}</strong>
              </div>
              <div>
                <span>Area</span>
                <strong>{renderArea(detail.areaAcres, detail.areaM2)}</strong>
              </div>
              <div>
                <span>Map geometry</span>
                <strong>{detail.hasGeometry ? "Available" : "Not linked to project row"}</strong>
              </div>
            </div>
          </>
        ) : (
          <>
            <h2>Planning Block {detail.planningBlock}</h2>
            <p className="status">Planning block context</p>
            <div className="detail-list">
              <div>
                <span>Planning block</span>
                <strong>{detail.planningBlock}</strong>
              </div>
              <div>
                <span>Zoning name</span>
                <strong>{renderValue(detail.zoningName)}</strong>
              </div>
              <div>
                <span>Mukim</span>
                <strong>{renderValue(detail.mukim)}</strong>
              </div>
              <div>
                <span>Planning block area</span>
                <strong>
                  {detail.areaAcres.toFixed(2)} ac / {detail.areaM2.toFixed(0)} m2
                </strong>
              </div>
              <div>
                <span>Development files</span>
                <strong>{detail.featureCount}</strong>
              </div>
              <div>
                <span>Approved files</span>
                <strong>{detail.approvedCount}</strong>
              </div>
              <div>
                <span>Pending files</span>
                <strong>{detail.pendingCount}</strong>
              </div>
              <div>
                <span>Development area</span>
                <strong>{detail.totalDevelopmentAreaAcres.toFixed(2)} ac</strong>
              </div>
              <div>
                <span>Layer mix</span>
                <strong>{renderValue(layerBreakdown)}</strong>
              </div>
            </div>
          </>
        )
      ) : (
        <p className="muted">
          Click a development polygon, search result, or planning block dot to inspect the reviewed public attributes.
        </p>
      )}
    </section>
  );
}
