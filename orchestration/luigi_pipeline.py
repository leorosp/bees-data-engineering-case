from __future__ import annotations

import json
import time
from pathlib import Path

import luigi

from bees_case.api_runner import SourceResolution, build_api_run_config, resolve_source_records
from bees_case.pyspark_local import (
    build_bronze_df,
    build_gold_df,
    build_output_paths,
    build_quality_dfs,
    build_silver_df,
    create_spark_session,
    write_bronze_df,
    write_gold_df,
    write_ops_dfs,
    write_silver_df,
)
from bees_case.quality import enforce_quality_gate


def run_with_retries(operation, attempts: int, delay_seconds: int = 1):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as error:  # pragma: no cover - retry branch is best-effort
            last_error = error
            if attempt == attempts:
                break
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error


class BasePipelineTask(luigi.Task):
    source_mode = luigi.Parameter(default="api")
    source_file = luigi.Parameter(default="examples/sample_breweries.json")
    source_api_base_url = luigi.Parameter(default="https://api.openbrewerydb.org/v1/breweries")
    fallback_to_sample = luigi.BoolParameter(default=True)
    per_page = luigi.IntParameter(default=200)
    max_pages = luigi.IntParameter(default=25)
    output_dir = luigi.Parameter(default="luigi_output")
    landing_date = luigi.Parameter(default="2026-03-16")
    run_id = luigi.Parameter(default="luigi-run-001")
    retries = luigi.IntParameter(default=2, significant=False)
    retry_delay_seconds = luigi.IntParameter(default=1, significant=False)

    def output_paths(self) -> dict[str, Path]:
        return build_output_paths(self.output_dir, self.landing_date)

    def marker_path(self, name: str) -> Path:
        return Path(self.output_dir) / "_orchestration" / f"{name}.json"

    def write_marker(self, name: str, payload: dict) -> None:
        marker = self.marker_path(name)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def run_step(self, operation):
        return run_with_retries(
            operation,
            attempts=self.retries + 1,
            delay_seconds=self.retry_delay_seconds,
        )

    def load_source_records(self) -> SourceResolution:
        config = build_api_run_config(
            output_root=self.output_dir,
            landing_date=self.landing_date,
            run_id=self.run_id,
            source_mode=self.source_mode,
            source_api_base_url=self.source_api_base_url,
            sample_file=self.source_file,
            fallback_to_sample=self.fallback_to_sample,
            per_page=self.per_page,
            max_pages=self.max_pages,
            api_timeout_seconds=30,
            api_request_retries=1,
        )
        return resolve_source_records(config)


class BronzeTask(BasePipelineTask):
    def output(self):
        return luigi.LocalTarget(str(self.marker_path("bronze")))

    def run(self):
        def operation():
            spark = create_spark_session("bees-case-bronze")
            try:
                source = self.load_source_records()
                bronze_df = build_bronze_df(
                    spark=spark,
                    source_records=source.records,
                    landing_date=self.landing_date,
                    run_id=self.run_id,
                )
                write_bronze_df(bronze_df, self.output_paths()["bronze"])
                payload = {
                    "bronze_output_path": str(self.output_paths()["bronze"]),
                    "source_record_count": len(source.records),
                    "requested_source_mode": source.requested_source_mode,
                    "source_type": source.source_type,
                    "fallback_used": source.fallback_used,
                    "fallback_reason": source.fallback_reason,
                    "source_api_base_url": source.source_api_base_url,
                    "sample_file": source.sample_file,
                    "pages_requested": source.pages_requested,
                    "pages_with_data": source.pages_with_data,
                    "records_fetched": source.records_fetched,
                    "run_id": self.run_id,
                }
                self.write_marker("bronze", payload)
            finally:
                spark.stop()

        self.run_step(operation)


class SilverTask(BasePipelineTask):
    def requires(self):
        return BronzeTask(
            source_mode=self.source_mode,
            source_file=self.source_file,
            source_api_base_url=self.source_api_base_url,
            fallback_to_sample=self.fallback_to_sample,
            per_page=self.per_page,
            max_pages=self.max_pages,
            output_dir=self.output_dir,
            landing_date=self.landing_date,
            run_id=self.run_id,
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
        )

    def output(self):
        return luigi.LocalTarget(str(self.marker_path("silver")))

    def run(self):
        def operation():
            spark = create_spark_session("bees-case-silver")
            try:
                bronze_df = spark.read.json(str(self.output_paths()["bronze"]))
                silver_df = build_silver_df(bronze_df)
                write_silver_df(silver_df, self.output_paths()["silver"])
                payload = {
                    "silver_output_path": str(self.output_paths()["silver"]),
                    "silver_record_count": silver_df.count(),
                    "partitions": ["country", "state_province"],
                    "run_id": self.run_id,
                }
                self.write_marker("silver", payload)
            finally:
                spark.stop()

        self.run_step(operation)


class GoldTask(BasePipelineTask):
    def requires(self):
        return SilverTask(
            source_mode=self.source_mode,
            source_file=self.source_file,
            source_api_base_url=self.source_api_base_url,
            fallback_to_sample=self.fallback_to_sample,
            per_page=self.per_page,
            max_pages=self.max_pages,
            output_dir=self.output_dir,
            landing_date=self.landing_date,
            run_id=self.run_id,
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
        )

    def output(self):
        return luigi.LocalTarget(str(self.marker_path("gold")))

    def run(self):
        def operation():
            spark = create_spark_session("bees-case-gold")
            try:
                silver_df = spark.read.parquet(str(self.output_paths()["silver"]))
                gold_df = build_gold_df(silver_df, self.run_id)
                write_gold_df(gold_df, self.output_paths()["gold"])
                payload = {
                    "gold_output_path": str(self.output_paths()["gold"]),
                    "gold_record_count": gold_df.count(),
                    "run_id": self.run_id,
                }
                self.write_marker("gold", payload)
            finally:
                spark.stop()

        self.run_step(operation)


class OpsTask(BasePipelineTask):
    fail_on_critical_quality = luigi.BoolParameter(default=False)

    def requires(self):
        return {
            "bronze": BronzeTask(
                source_mode=self.source_mode,
                source_file=self.source_file,
                source_api_base_url=self.source_api_base_url,
                fallback_to_sample=self.fallback_to_sample,
                per_page=self.per_page,
                max_pages=self.max_pages,
                output_dir=self.output_dir,
                landing_date=self.landing_date,
                run_id=self.run_id,
                retries=self.retries,
                retry_delay_seconds=self.retry_delay_seconds,
            ),
            "silver": SilverTask(
                source_mode=self.source_mode,
                source_file=self.source_file,
                source_api_base_url=self.source_api_base_url,
                fallback_to_sample=self.fallback_to_sample,
                per_page=self.per_page,
                max_pages=self.max_pages,
                output_dir=self.output_dir,
                landing_date=self.landing_date,
                run_id=self.run_id,
                retries=self.retries,
                retry_delay_seconds=self.retry_delay_seconds,
            ),
            "gold": GoldTask(
                source_mode=self.source_mode,
                source_file=self.source_file,
                source_api_base_url=self.source_api_base_url,
                fallback_to_sample=self.fallback_to_sample,
                per_page=self.per_page,
                max_pages=self.max_pages,
                output_dir=self.output_dir,
                landing_date=self.landing_date,
                run_id=self.run_id,
                retries=self.retries,
                retry_delay_seconds=self.retry_delay_seconds,
            ),
        }

    def output(self):
        return luigi.LocalTarget(str(self.marker_path("ops")))

    def run(self):
        def operation():
            spark = create_spark_session("bees-case-ops")
            try:
                source = self.load_source_records()
                bronze_df = spark.read.json(str(self.output_paths()["bronze"]))
                silver_df = spark.read.parquet(str(self.output_paths()["silver"]))
                gold_df = spark.read.parquet(str(self.output_paths()["gold"]))
                quality_df, execution_df, quality_results = build_quality_dfs(
                    spark=spark,
                    bronze_df=bronze_df,
                    silver_df=silver_df,
                    gold_df=gold_df,
                    run_id=self.run_id,
                    source_metadata=source.as_metadata(),
                )
                write_ops_dfs(
                    quality_df,
                    execution_df,
                    self.output_paths()["quality"],
                    self.output_paths()["execution"],
                )
                if self.fail_on_critical_quality:
                    enforce_quality_gate(quality_results)
                payload = {
                    "quality_results_path": str(self.output_paths()["quality"]),
                    "execution_events_path": str(self.output_paths()["execution"]),
                    "quality_gate_status": "pass"
                    if execution_df.collect()[0]["status"] == "success"
                    else "fail",
                    "run_id": self.run_id,
                }
                self.write_marker("ops", payload)
            finally:
                spark.stop()

        self.run_step(operation)


class PipelineOrchestration(BasePipelineTask):
    fail_on_critical_quality = luigi.BoolParameter(default=False)

    def requires(self):
        return OpsTask(
            source_mode=self.source_mode,
            source_file=self.source_file,
            source_api_base_url=self.source_api_base_url,
            fallback_to_sample=self.fallback_to_sample,
            per_page=self.per_page,
            max_pages=self.max_pages,
            output_dir=self.output_dir,
            landing_date=self.landing_date,
            run_id=self.run_id,
            retries=self.retries,
            retry_delay_seconds=self.retry_delay_seconds,
            fail_on_critical_quality=self.fail_on_critical_quality,
        )

    def output(self):
        return luigi.LocalTarget(str(self.marker_path("pipeline_summary")))

    def run(self):
        ops_payload = json.loads(Path(self.input().path).read_text(encoding="utf-8"))
        summary = {
            "source_mode": self.source_mode,
            "source_file": self.source_file,
            "source_api_base_url": self.source_api_base_url,
            "fallback_to_sample": self.fallback_to_sample,
            "output_dir": self.output_dir,
            "landing_date": self.landing_date,
            "run_id": self.run_id,
            "orchestrator": "luigi",
            "retries": self.retries,
            "fail_on_critical_quality": self.fail_on_critical_quality,
            **ops_payload,
        }
        self.write_marker("pipeline_summary", summary)
