"use client";

import { useDeferredValue, useEffect, useState } from "react";

import type { SearchResult } from "@/lib/types";

type Props = {
  onSelect: (result: SearchResult) => void;
};

export function SearchBox({ onSelect }: Props) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [hasCompletedSearch, setHasCompletedSearch] = useState(false);
  const deferredQuery = useDeferredValue(query);

  useEffect(() => {
    if (!deferredQuery.trim()) {
      setResults([]);
      setLoading(false);
      setHasCompletedSearch(false);
      return;
    }

    const controller = new AbortController();
    setHasCompletedSearch(false);

    const timeout = setTimeout(async () => {
      setLoading(true);
      try {
        const response = await fetch(`/api/v1/search?q=${encodeURIComponent(deferredQuery)}`, {
          signal: controller.signal
        });
        const payload = (await response.json()) as SearchResult[];
        if (controller.signal.aborted) {
          return;
        }
        setResults(payload);
      } catch {
        if (controller.signal.aborted) {
          return;
        }
        setResults([]);
      } finally {
        if (controller.signal.aborted) {
          return;
        }
        setLoading(false);
        setHasCompletedSearch(true);
      }
    }, 180);

    return () => {
      controller.abort();
      clearTimeout(timeout);
    };
  }, [deferredQuery]);

  return (
    <section className="search-box">
      <div>
        <p className="eyebrow">Search</p>
        <input
          aria-label="Search MBJB files"
          placeholder="Reference no, title, lot, mukim, planning block"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      {loading ? <div className="muted">Searching...</div> : null}
      {!loading && hasCompletedSearch && deferredQuery && results.length === 0 ? (
        <div className="muted">No matching files.</div>
      ) : null}
      {results.length ? (
        <div className="search-results">
          {results.map((result) => (
            <button
              key={result.applicationId}
              className="result-item"
              type="button"
              onClick={() => {
                onSelect(result);
                setQuery(result.referenceNo ?? result.title);
                setResults([]);
                setHasCompletedSearch(false);
              }}
            >
              <strong>{result.referenceNo ?? result.title}</strong>
              <span className="result-meta">{result.title}</span>
              <span className="result-meta">
                {result.layerType.replaceAll("_", " ")} | {result.status}
              </span>
            </button>
          ))}
        </div>
      ) : null}
    </section>
  );
}
