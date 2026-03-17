from pathlib import Path

from pyspark.sql import SparkSession

from bees_case.bronze import fetch_all_pages
from bees_case.config import PipelineRunConfig
from bees_case.pyspark_local import run_local_pyspark_pipeline_from_records


def build_api_run_config(
    *,
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    source_api_base_url: str,
    per_page: int,
    max_pages: int,
) -> PipelineRunConfig:
    return PipelineRunConfig(
        base_path=Path(output_root).as_posix(),
        source_api_base_url=source_api_base_url,
        per_page=per_page,
        max_pages=max_pages,
        landing_date=landing_date,
        run_id=run_id,
    )


def fetch_api_records(config: PipelineRunConfig) -> list[dict]:
    records = fetch_all_pages(config)
    if not records:
        raise ValueError("No records were returned by the Open Brewery DB API.")
    return records


def run_api_pyspark_pipeline(
    *,
    spark: SparkSession,
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    source_api_base_url: str = "https://api.openbrewerydb.org/v1/breweries",
    per_page: int = 200,
    max_pages: int = 25,
    fail_on_critical_quality: bool = False,
) -> dict:
    config = build_api_run_config(
        output_root=output_root,
        landing_date=landing_date,
        run_id=run_id,
        source_api_base_url=source_api_base_url,
        per_page=per_page,
        max_pages=max_pages,
    )
    records = fetch_api_records(config)
    summary = run_local_pyspark_pipeline_from_records(
        spark=spark,
        source_records=records,
        output_root=output_root,
        landing_date=landing_date,
        run_id=run_id,
        fail_on_critical_quality=fail_on_critical_quality,
    )
    return {
        **summary,
        "source_mode": "api",
        "source_api_base_url": source_api_base_url,
        "per_page": per_page,
        "max_pages": max_pages,
    }
