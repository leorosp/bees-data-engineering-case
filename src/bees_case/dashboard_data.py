from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class DashboardData:
    gold: pd.DataFrame
    quality: pd.DataFrame
    execution: pd.DataFrame
    source_root: Path


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    if not directory.exists():
        raise FileNotFoundError(f"Missing artifact directory: {directory}")
    return pd.read_parquet(directory)


def load_dashboard_data(output_root: str | Path) -> DashboardData:
    root = Path(output_root)
    gold = _read_parquet_dir(root / "gold" / "breweries_by_type_location")
    quality = _read_parquet_dir(root / "ops" / "quality_results")
    execution = _read_parquet_dir(root / "ops" / "execution_events")

    if "generated_at_utc" in gold.columns:
        gold["generated_at_utc"] = pd.to_datetime(gold["generated_at_utc"], errors="coerce")
    if "checked_at_utc" in quality.columns:
        quality["checked_at_utc"] = pd.to_datetime(quality["checked_at_utc"], errors="coerce")
    if "event_timestamp_utc" in execution.columns:
        execution["event_timestamp_utc"] = pd.to_datetime(
            execution["event_timestamp_utc"],
            errors="coerce",
        )

    return DashboardData(
        gold=gold,
        quality=quality,
        execution=execution,
        source_root=root,
    )
