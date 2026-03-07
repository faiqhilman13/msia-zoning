import { Suspense } from "react";

import { MapShell } from "@/components/map-shell";

export default function HomePage() {
  return (
    <Suspense fallback={<main className="sources-page">Loading map…</main>}>
      <MapShell />
    </Suspense>
  );
}
