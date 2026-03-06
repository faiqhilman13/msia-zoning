from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ArcGISLayerConfig:
    slug: str
    label: str
    service_name: str
    layer_id: int
    application_type: str
    kind: str = "development"
    object_id_field: str = "OBJECTID"
    field_map: dict[str, str] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        return f"{self.service_name}/FeatureServer/{self.layer_id}"


@dataclass(frozen=True)
class ArcGISArtifact:
    artifact_type: str
    relative_path: str
    sha256: str
    file_size_bytes: int


JSONDict = dict[str, Any]
