import type { DistributionRow, OverviewStats } from "@/lib/types";

type Props = {
  overview: OverviewStats | null;
  byLayer: DistributionRow[];
};

export function StatsBar({ overview, byLayer }: Props) {
  const layerTotal = byLayer.reduce((total, row) => total + row.count, 0);
  return (
    <section className="summary-bar">
      <article className="stat-card">
        <span className="stat-label">Visible records</span>
        <strong className="stat-value">{overview?.totalFeatures ?? 0}</strong>
      </article>
      <article className="stat-card">
        <span className="stat-label">Approved</span>
        <strong className="stat-value">{overview?.approvedFeatures ?? 0}</strong>
      </article>
      <article className="stat-card">
        <span className="stat-label">Pending</span>
        <strong className="stat-value">{overview?.pendingFeatures ?? 0}</strong>
      </article>
      <article className="stat-card">
        <span className="stat-label">Visible area</span>
        <strong className="stat-value">{overview?.totalAreaAcres?.toFixed(1) ?? "0.0"} ac</strong>
      </article>
      <article className="stat-card">
        <span className="stat-label">Layer total</span>
        <strong className="stat-value">{layerTotal}</strong>
      </article>
    </section>
  );
}
