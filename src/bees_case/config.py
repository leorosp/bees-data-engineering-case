from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class LayerPaths:
    bronze: str
    silver: str
    gold: str
    ops: str


@dataclass(frozen=True)
class PipelineRunConfig:
    base_path: str
    source_mode: str = "api"
    source_api_base_url: str = "https://api.openbrewerydb.org/v1/breweries"
    sample_file: str = "examples/sample_breweries.json"
    fallback_to_sample: bool = True
    per_page: int = 200
    max_pages: int = 25
    api_timeout_seconds: int = 30
    api_request_retries: int = 1
    landing_date: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    run_id: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    )

    def build_paths(self) -> LayerPaths:
        root = self.base_path.rstrip("/")
        return LayerPaths(
            bronze=f"{root}/bronze",
            silver=f"{root}/silver",
            gold=f"{root}/gold",
            ops=f"{root}/ops",
        )
