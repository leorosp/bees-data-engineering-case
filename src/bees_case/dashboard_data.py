from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class DashboardData:
    gold: pd.DataFrame
    quality: pd.DataFrame
    execution: pd.DataFrame
    source_root: Path
    source_mode: str


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    if not directory.exists():
        raise FileNotFoundError(f"Missing artifact directory: {directory}")
    return pd.read_parquet(directory)


def _read_dataframe(path: Path) -> pd.DataFrame:
    if path.is_dir():
        return _read_parquet_dir(path)
    if path.is_file() and path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise FileNotFoundError(f"Missing artifact path: {path}")


def _normalize_dashboard_data(
    *,
    gold: pd.DataFrame,
    quality: pd.DataFrame,
    execution: pd.DataFrame,
    source_root: Path,
    source_mode: str,
) -> DashboardData:
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
        source_root=source_root,
        source_mode=source_mode,
    )


def load_dashboard_data(output_root: str | Path) -> DashboardData:
    root = Path(output_root)
    gold = _read_dataframe(root / "gold" / "breweries_by_type_location")
    quality = _read_dataframe(root / "ops" / "quality_results")
    execution = _read_dataframe(root / "ops" / "execution_events")
    return _normalize_dashboard_data(
        gold=gold,
        quality=quality,
        execution=execution,
        source_root=root,
        source_mode="artifacts",
    )


def load_demo_dashboard_data(demo_root: str | Path) -> DashboardData:
    root = Path(demo_root)
    gold = _read_dataframe(root / "gold" / "breweries_by_type_location.csv")
    quality = _read_dataframe(root / "ops" / "quality_results.csv")
    execution = _read_dataframe(root / "ops" / "execution_events.csv")
    return _normalize_dashboard_data(
        gold=gold,
        quality=quality,
        execution=execution,
        source_root=root,
        source_mode="demo",
    )
