from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from bees_case.api_client import ApiFetchError, ApiFetchResult, fetch_all_pages
from bees_case.config import PipelineRunConfig
from bees_case.pyspark_local import load_records, run_local_pyspark_pipeline_from_records


@dataclass(frozen=True)
class SourceResolution:
    records: list[dict]
    requested_source_mode: str
    source_type: str
    fallback_used: bool
    fallback_reason: str
    source_api_base_url: str
    sample_file: str
    pages_requested: int
    pages_with_data: int
    records_fetched: int

    def as_metadata(self) -> dict:
        return asdict(self)


def build_api_run_config(
    *,
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    source_mode: str,
    source_api_base_url: str,
    sample_file: str,
    fallback_to_sample: bool,
    per_page: int,
    max_pages: int,
    api_timeout_seconds: int,
    api_request_retries: int,
) -> PipelineRunConfig:
    return PipelineRunConfig(
        base_path=Path(output_root).as_posix(),
        source_mode=source_mode,
        source_api_base_url=source_api_base_url,
        sample_file=sample_file,
        fallback_to_sample=fallback_to_sample,
        per_page=per_page,
        max_pages=max_pages,
        api_timeout_seconds=api_timeout_seconds,
        api_request_retries=api_request_retries,
        landing_date=landing_date,
        run_id=run_id,
    )


def fetch_api_records(config: PipelineRunConfig) -> ApiFetchResult:
    result = fetch_all_pages(config)
    if not result.records:
        raise ApiFetchError(
            "No records were returned by the Open Brewery DB API.",
            pages_requested=result.pages_requested,
            records_fetched=result.records_fetched,
        )
    return result


def resolve_source_records(config: PipelineRunConfig) -> SourceResolution:
    if config.source_mode == "sample":
        sample_records = load_records(config.sample_file)
        return SourceResolution(
            records=sample_records,
            requested_source_mode=config.source_mode,
            source_type="sample",
            fallback_used=False,
            fallback_reason="",
            source_api_base_url=config.source_api_base_url,
            sample_file=config.sample_file,
            pages_requested=0,
            pages_with_data=0,
            records_fetched=len(sample_records),
        )

    if config.source_mode not in {"api", "auto"}:
        raise ValueError("source_mode must be 'api', 'sample' or 'auto'.")

    try:
        api_result = fetch_api_records(config)
        return SourceResolution(
            records=api_result.records,
            requested_source_mode=config.source_mode,
            source_type="api",
            fallback_used=False,
            fallback_reason="",
            source_api_base_url=config.source_api_base_url,
            sample_file=config.sample_file,
            pages_requested=api_result.pages_requested,
            pages_with_data=api_result.pages_with_data,
            records_fetched=api_result.records_fetched,
        )
    except ApiFetchError as error:
        if not config.fallback_to_sample:
            raise
        sample_records = load_records(config.sample_file)
        reason = (
            "api_empty_response"
            if "No records were returned" in str(error)
            else "api_request_failed"
        )
        return SourceResolution(
            records=sample_records,
            requested_source_mode=config.source_mode,
            source_type="sample",
            fallback_used=True,
            fallback_reason=reason,
            source_api_base_url=config.source_api_base_url,
            sample_file=config.sample_file,
            pages_requested=error.pages_requested,
            pages_with_data=0,
            records_fetched=len(sample_records),
        )


def run_api_pyspark_pipeline(
    *,
    spark: SparkSession,
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    source_mode: str = "api",
    source_api_base_url: str = "https://api.openbrewerydb.org/v1/breweries",
    sample_file: str = "examples/sample_breweries.json",
    fallback_to_sample: bool = True,
    per_page: int = 200,
    max_pages: int = 25,
    api_timeout_seconds: int = 30,
    api_request_retries: int = 1,
    fail_on_critical_quality: bool = False,
) -> dict:
    config = build_api_run_config(
        output_root=output_root,
        landing_date=landing_date,
        run_id=run_id,
        source_mode=source_mode,
        source_api_base_url=source_api_base_url,
        sample_file=sample_file,
        fallback_to_sample=fallback_to_sample,
        per_page=per_page,
        max_pages=max_pages,
        api_timeout_seconds=api_timeout_seconds,
        api_request_retries=api_request_retries,
    )
    source = resolve_source_records(config)
    summary = run_local_pyspark_pipeline_from_records(
        spark=spark,
        source_records=source.records,
        output_root=output_root,
        landing_date=landing_date,
        run_id=run_id,
        fail_on_critical_quality=fail_on_critical_quality,
        source_metadata=source.as_metadata(),
    )
    return {
        **summary,
        "requested_source_mode": source.requested_source_mode,
        "source_type": source.source_type,
        "fallback_used": source.fallback_used,
        "fallback_reason": source.fallback_reason,
        "source_api_base_url": source_api_base_url,
        "sample_file": sample_file,
        "per_page": per_page,
        "max_pages": max_pages,
    }
