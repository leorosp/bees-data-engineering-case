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


dbutils.widgets.text("base_path", "")
dbutils.widgets.text("run_id", "")


base_path = dbutils.widgets.get("base_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not base_path:
    raise ValueError("Parameter 'base_path' is required.")

defaults = PipelineRunConfig(base_path=base_path)
config = PipelineRunConfig(
    base_path=base_path,
    run_id=run_id or defaults.run_id,
)
paths = config.build_paths()

silver_input_path = f"{paths.silver}/breweries"
gold_output_path = f"{paths.gold}/breweries_by_type_location"

silver_df = spark.read.format("delta").load(silver_input_path)

gold_df = (
    silver_df.groupBy("brewery_type", "country", "state_province")
    .agg(F.countDistinct("brewery_id").alias("brewery_count"))
    .withColumn("run_id", F.lit(config.run_id))
    .withColumn("generated_at_utc", F.current_timestamp())
)

gold_df.write.format("delta").mode("overwrite").save(gold_output_path)

dbutils.notebook.exit(
    json.dumps(
        {
            "gold_output_path": gold_output_path,
            "run_id": config.run_id,
            "row_count": gold_df.count(),
        }
    )
)
