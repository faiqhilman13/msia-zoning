from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=False)


@dataclass(frozen=True)
class AppConfig:
    root: Path = ROOT
    app_env: str = os.getenv("APP_ENV", "development")
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/permits"
    )
    mbjb_service_root: str = os.getenv(
        "MBJB_SERVICE_ROOT", "https://geojb.gov.my/gisserver/rest/services/GEOJB"
    )
    mbjb_max_batch_size: int = int(os.getenv("MBJB_MAX_BATCH_SIZE", "200"))
    mbjb_request_timeout_seconds: int = int(
        os.getenv("MBJB_REQUEST_TIMEOUT_SECONDS", "60")
    )
    mbjb_retry_attempts: int = int(os.getenv("MBJB_RETRY_ATTEMPTS", "4"))
    user_agent: str = os.getenv(
        "MBJB_USER_AGENT", "malaysia-permits-map/0.1 (+https://localhost)"
    )

    @property
    def data_raw_dir(self) -> Path:
        return self.root / "data" / "raw" / "mbjb"

    @property
    def data_stage_dir(self) -> Path:
        return self.root / "data" / "stage" / "mbjb"

    @property
    def data_publish_dir(self) -> Path:
        return self.root / "data" / "publish"

    @property
    def migrations_dir(self) -> Path:
        return self.root / "infra" / "migrations"


CONFIG = AppConfig()
