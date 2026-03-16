# Databricks notebook source
import json
import sys
from pathlib import Path

from pyspark.sql import functions as F


def add_repo_src_to_path() -> str:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_notebook_path = Path("/Workspace") / notebook_path.lstrip("/")
    checked_paths: list[str] = []
    for candidate in [workspace_notebook_path.parent, *workspace_notebook_path.parents]:
        src_path = candidate / "src"
        checked_paths.append(str(src_path))
        try:
            if (src_path / "bees_case").is_dir():
                resolved_src_path = str(src_path)
                if resolved_src_path not in sys.path:
                    sys.path.append(resolved_src_path)
                return resolved_src_path
        except OSError:
            continue
    raise FileNotFoundError(
        "Could not resolve the repository src path. "
        f"Checked: {checked_paths}. Run this notebook from the Databricks Git folder."
    )


add_repo_src_to_path()

from bees_case.config import PipelineRunConfig
from bees_case.contracts import REQUIRED_BREWERY_FIELDS
from bees_case.observability import build_execution_event
from bees_case.quality import (
    build_quality_result,
    has_duplicate_primary_keys,
    summarize_required_field_gaps,
)


dbutils.widgets.text("base_path", "")
dbutils.widgets.text("landing_date", "")
dbutils.widgets.text("run_id", "")


base_path = dbutils.widgets.get("base_path").strip()
landing_date = dbutils.widgets.get("landing_date").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not base_path:
    raise ValueError("Parameter 'base_path' is required.")

defaults = PipelineRunConfig(base_path=base_path)
config = PipelineRunConfig(
    base_path=base_path,
    landing_date=landing_date or defaults.landing_date,
    run_id=run_id or defaults.run_id,
)
paths = config.build_paths()

bronze_df = spark.read.json(f"{paths.bronze}/landing_date={config.landing_date}")
silver_df = spark.read.format("delta").load(f"{paths.silver}/breweries")
gold_df = spark.read.format("delta").load(f"{paths.gold}/breweries_by_type_location")

silver_records = [
    row.asDict()
    for row in silver_df.select(
        "brewery_id",
        "name",
        "brewery_type",
        "city",
        "country",
    ).collect()
]

field_gaps = summarize_required_field_gaps(silver_records, REQUIRED_BREWERY_FIELDS)
bronze_record_ids = [
    row["record_id"]
    for row in bronze_df.select("record_id").collect()
    if row["record_id"]
]
has_duplicates = has_duplicate_primary_keys(bronze_record_ids)
negative_gold_counts = gold_df.filter(F.col("brewery_count") < 0).count()

quality_results = [
    build_quality_result(
        layer="silver",
        check_name="required_fields",
        status="pass" if sum(field_gaps.values()) == 0 else "fail",
        metric_name="missing_required_fields",
        metric_value=sum(field_gaps.values()),
        run_id=config.run_id,
        message=json.dumps(field_gaps),
    ),
    build_quality_result(
        layer="bronze",
        check_name="duplicate_primary_keys",
        status="fail" if has_duplicates else "pass",
        metric_name="duplicate_primary_keys",
        metric_value=1 if has_duplicates else 0,
        run_id=config.run_id,
        message="Duplicate record_id values found in bronze." if has_duplicates else "No duplicates found in bronze.",
    ),
    build_quality_result(
        layer="gold",
        check_name="negative_brewery_count",
        status="fail" if negative_gold_counts else "pass",
        metric_name="negative_brewery_count",
        metric_value=negative_gold_counts,
        run_id=config.run_id,
        message="Gold aggregations must not produce negative counts.",
    ),
]

execution_events = [
    build_execution_event(
        layer="ops",
        stage="quality_checks",
        status="success",
        run_id=config.run_id,
        records_in=len(silver_records),
        records_out=len(quality_results),
        details="Quality checks completed for silver and gold layers.",
    )
]

spark.createDataFrame(quality_results).write.format("delta").mode("append").save(
    f"{paths.ops}/quality_results"
)

spark.createDataFrame(execution_events).write.format("delta").mode("append").save(
    f"{paths.ops}/execution_events"
)

dbutils.notebook.exit(
    json.dumps(
        {
            "quality_results_path": f"{paths.ops}/quality_results",
            "execution_events_path": f"{paths.ops}/execution_events",
            "run_id": config.run_id,
        }
    )
)
