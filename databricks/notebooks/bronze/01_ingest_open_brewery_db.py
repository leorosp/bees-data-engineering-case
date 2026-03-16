# Databricks notebook source
import json
import sys
from pathlib import Path


def add_repo_src_to_path() -> str:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_notebook_path = Path("/Workspace") / notebook_path.lstrip("/")
    for candidate in [workspace_notebook_path.parent, *workspace_notebook_path.parents]:
        src_path = candidate / "src"
        if (src_path / "bees_case").exists():
            resolved_src_path = str(src_path)
            if resolved_src_path not in sys.path:
                sys.path.append(resolved_src_path)
            return resolved_src_path
    raise FileNotFoundError(
        "Could not resolve the repository src path. Run this notebook from the Databricks Git folder."
    )


add_repo_src_to_path()

from bees_case.bronze import build_bronze_rows, fetch_all_pages
from bees_case.config import PipelineRunConfig


dbutils.widgets.text("base_path", "")
dbutils.widgets.text("landing_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("per_page", "200")
dbutils.widgets.text("max_pages", "25")


base_path = dbutils.widgets.get("base_path").strip()
landing_date = dbutils.widgets.get("landing_date").strip()
run_id = dbutils.widgets.get("run_id").strip()
per_page = int(dbutils.widgets.get("per_page") or "200")
max_pages = int(dbutils.widgets.get("max_pages") or "25")

if not base_path:
    raise ValueError("Parameter 'base_path' is required.")

defaults = PipelineRunConfig(base_path=base_path)
config = PipelineRunConfig(
    base_path=base_path,
    landing_date=landing_date or defaults.landing_date,
    run_id=run_id or defaults.run_id,
    per_page=per_page,
    max_pages=max_pages,
)

records = fetch_all_pages(config)
bronze_rows = build_bronze_rows(records, config.landing_date, config.run_id)

if not bronze_rows:
    raise ValueError("No records were returned by the source API.")

paths = config.build_paths()
bronze_output_path = f"{paths.bronze}/landing_date={config.landing_date}"

bronze_df = spark.createDataFrame(bronze_rows)
bronze_df.write.mode("overwrite").json(bronze_output_path)

dbutils.notebook.exit(
    json.dumps(
        {
            "bronze_output_path": bronze_output_path,
            "run_id": config.run_id,
            "row_count": len(bronze_rows),
        }
    )
)
