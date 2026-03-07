from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

import httpx
import orjson

from malaysia_permits_map.config import AppConfig
from malaysia_permits_map.models.arcgis import ArcGISArtifact, ArcGISLayerConfig, JSONDict
from malaysia_permits_map.utils.paths import ensure_directory


class ArcGISClient:
    def __init__(
        self,
        config: AppConfig,
        service_root: str | None = None,
        timeout_seconds: int | None = None,
        retry_attempts: int | None = None,
    ) -> None:
        self.config = config
        self.service_root = service_root or config.mbjb_service_root
        self.retry_attempts = retry_attempts or config.mbjb_retry_attempts
        self.client = httpx.Client(
            timeout=timeout_seconds or config.mbjb_request_timeout_seconds,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def _request(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                return response
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.retry_attempts:
                    break
                time.sleep(2**attempt)
        raise RuntimeError(f"Request failed for {url}") from last_error

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> JSONDict:
        response = self._request(url, params=params)
        return response.json()

    def write_bytes(self, path: Path, content: bytes) -> ArcGISArtifact:
        ensure_directory(path.parent)
        path.write_bytes(content)
        return ArcGISArtifact(
            artifact_type=path.suffix.lstrip(".") or "file",
            relative_path=str(path),
            sha256=hashlib.sha256(content).hexdigest(),
            file_size_bytes=len(content),
        )

    def fetch_layer_metadata(self, layer: ArcGISLayerConfig) -> JSONDict:
        return self.get_json(
            f"{self.service_root}/{layer.base_url}",
            params={"f": "pjson"},
        )

    def fetch_count(self, layer: ArcGISLayerConfig) -> int:
        payload = self.get_json(
            f"{self.service_root}/{layer.base_url}/query",
            params={"where": "1=1", "returnCountOnly": "true", "f": "pjson"},
        )
        return int(payload["count"])

    def fetch_ids(self, layer: ArcGISLayerConfig) -> list[int]:
        payload = self.get_json(
            f"{self.service_root}/{layer.base_url}/query",
            params={"where": "1=1", "returnIdsOnly": "true", "f": "pjson"},
        )
        return [int(item) for item in payload.get("objectIds", [])]

    def fetch_geojson_batch(self, layer: ArcGISLayerConfig, object_ids: list[int]) -> bytes:
        response = self._request(
            f"{self.service_root}/{layer.base_url}/query",
            params={
                "objectIds": ",".join(str(item) for item in object_ids),
                "outFields": "*",
                "returnGeometry": "true",
                "f": "geojson",
            },
        )
        return response.content


def write_json(path: Path, payload: dict[str, Any]) -> ArcGISArtifact:
    ensure_directory(path.parent)
    content = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    path.write_bytes(content)
    return ArcGISArtifact(
        artifact_type="json",
        relative_path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        file_size_bytes=len(content),
    )
