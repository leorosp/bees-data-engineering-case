import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bees_case.bronze import build_bronze_rows
from bees_case.contracts import REQUIRED_BREWERY_FIELDS
from bees_case.observability import build_execution_event
from bees_case.quality import (
    build_quality_result,
    enforce_quality_gate,
    find_failed_quality_checks,
    has_duplicate_primary_keys,
    summarize_required_field_gaps,
)


BREWERY_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("brewery_type", T.StringType()),
        T.StructField("street", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("state_province", T.StringType()),
        T.StructField("postal_code", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("longitude", T.StringType()),
        T.StructField("latitude", T.StringType()),
        T.StructField("phone", T.StringType()),
        T.StructField("website_url", T.StringType()),
    ]
)


def create_spark_session(app_name: str = "bees-case-local") -> SparkSession:
    # Spark can fail to resolve the runner hostname in CI unless these are pinned.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_records(source_path: str | Path) -> list[dict]:
    with Path(source_path).open("r", encoding="utf-8") as source_file:
        return json.load(source_file)


def build_output_paths(output_root: str | Path, landing_date: str) -> dict[str, Path]:
    root = Path(output_root)
    return {
        "bronze": root / "bronze" / f"landing_date={landing_date}",
        "silver": root / "silver" / "breweries",
        "gold": root / "gold" / "breweries_by_type_location",
        "quality": root / "ops" / "quality_results",
        "execution": root / "ops" / "execution_events",
    }


def build_bronze_df(
    spark: SparkSession,
    source_records: list[dict],
    landing_date: str,
    run_id: str,
) -> DataFrame:
    bronze_rows = build_bronze_rows(source_records, landing_date, run_id)
    return spark.createDataFrame(bronze_rows)


def build_silver_df(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df.withColumn("payload", F.from_json(F.col("raw_payload"), BREWERY_SCHEMA))
        .select(
            F.col("payload.id").alias("brewery_id"),
            F.col("payload.name").alias("name"),
            F.col("payload.brewery_type").alias("brewery_type"),
            F.col("payload.city").alias("city"),
            F.coalesce(F.col("payload.state_province"), F.lit("unknown")).alias(
                "state_province"
            ),
            F.coalesce(F.col("payload.country"), F.lit("unknown")).alias("country"),
            F.col("payload.postal_code").alias("postal_code"),
            F.col("payload.street").alias("street"),
            F.col("payload.website_url").alias("website_url"),
            F.col("payload.phone").alias("phone"),
            F.col("payload.longitude").cast("double").alias("longitude"),
            F.col("payload.latitude").cast("double").alias("latitude"),
            F.col("landing_date"),
            F.col("run_id"),
        )
        .dropDuplicates(["brewery_id"])
    )


def build_gold_df(silver_df: DataFrame, run_id: str) -> DataFrame:
    return (
        silver_df.groupBy("brewery_type", "country", "state_province")
        .agg(F.countDistinct("brewery_id").alias("brewery_count"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("generated_at_utc", F.current_timestamp())
    )


def build_quality_results(
    bronze_df: DataFrame,
    silver_df: DataFrame,
    gold_df: DataFrame,
    run_id: str,
) -> list[dict]:
    raw_records = [
        row.asDict()
        for row in bronze_df.withColumn(
            "payload",
            F.from_json(F.col("raw_payload"), BREWERY_SCHEMA),
        )
        .select(
            F.col("payload.id").alias("brewery_id"),
            F.col("payload.name").alias("name"),
            F.col("payload.brewery_type").alias("brewery_type"),
            F.col("payload.city").alias("city"),
            F.col("payload.country").alias("country"),
        )
        .collect()
    ]

    field_gaps = summarize_required_field_gaps(raw_records, REQUIRED_BREWERY_FIELDS)
    bronze_record_ids = [
        row["record_id"]
        for row in bronze_df.select("record_id").collect()
        if row["record_id"]
    ]
    has_duplicates = has_duplicate_primary_keys(bronze_record_ids)
    negative_gold_counts = gold_df.filter(F.col("brewery_count") < 0).count()

    return [
        build_quality_result(
            layer="silver",
            check_name="required_fields",
            status="pass" if sum(field_gaps.values()) == 0 else "fail",
            metric_name="missing_required_fields",
            metric_value=sum(field_gaps.values()),
            run_id=run_id,
            message=json.dumps(field_gaps, sort_keys=True),
        ),
        build_quality_result(
            layer="bronze",
            check_name="duplicate_primary_keys",
            status="fail" if has_duplicates else "pass",
            metric_name="duplicate_primary_keys",
            metric_value=1 if has_duplicates else 0,
            run_id=run_id,
            message=(
                "Duplicate record_id values found in bronze."
                if has_duplicates
                else "No duplicates found in bronze."
            ),
        ),
        build_quality_result(
            layer="gold",
            check_name="negative_brewery_count",
            status="fail" if negative_gold_counts else "pass",
            metric_name="negative_brewery_count",
            metric_value=negative_gold_counts,
            run_id=run_id,
            message="Gold aggregations must not produce negative counts.",
        ),
    ]


def build_quality_dfs(
    spark: SparkSession,
    bronze_df: DataFrame,
    silver_df: DataFrame,
    gold_df: DataFrame,
    run_id: str,
) -> tuple[DataFrame, DataFrame, list[dict]]:
    quality_results = build_quality_results(bronze_df, silver_df, gold_df, run_id)
    failed_critical_checks = find_failed_quality_checks(quality_results)

    execution_events = [
        build_execution_event(
            layer="ops",
            stage="local_pyspark_pipeline",
            status="failed" if failed_critical_checks else "success",
            run_id=run_id,
            records_in=bronze_df.count(),
            records_out=len(quality_results),
            details=(
                "Critical quality checks failed during the local or Colab PySpark validation run."
                if failed_critical_checks
                else "Local or Colab PySpark validation run completed successfully."
            ),
        )
    ]

    return (
        spark.createDataFrame(quality_results),
        spark.createDataFrame(execution_events),
        quality_results,
    )


def write_bronze_df(bronze_df: DataFrame, bronze_path: str | Path) -> None:
    bronze_df.write.mode("overwrite").json(str(bronze_path))


def write_silver_df(silver_df: DataFrame, silver_path: str | Path) -> None:
    silver_df.write.mode("overwrite").partitionBy("country", "state_province").parquet(
        str(silver_path)
    )


def write_gold_df(gold_df: DataFrame, gold_path: str | Path) -> None:
    gold_df.write.mode("overwrite").parquet(str(gold_path))


def write_ops_dfs(
    quality_df: DataFrame,
    execution_df: DataFrame,
    quality_path: str | Path,
    execution_path: str | Path,
) -> None:
    quality_df.write.mode("overwrite").parquet(str(quality_path))
    execution_df.write.mode("overwrite").parquet(str(execution_path))


def run_local_pyspark_pipeline(
    *,
    spark: SparkSession,
    source_path: str | Path,
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    fail_on_critical_quality: bool = False,
) -> dict:
    source_records = load_records(source_path)
    return run_local_pyspark_pipeline_from_records(
        spark=spark,
        source_records=source_records,
        output_root=output_root,
        landing_date=landing_date,
        run_id=run_id,
        fail_on_critical_quality=fail_on_critical_quality,
    )


def run_local_pyspark_pipeline_from_records(
    *,
    spark: SparkSession,
    source_records: list[dict],
    output_root: str | Path,
    landing_date: str,
    run_id: str,
    fail_on_critical_quality: bool = False,
) -> dict:
    paths = build_output_paths(output_root, landing_date)

    bronze_df = build_bronze_df(spark, source_records, landing_date, run_id)
    silver_df = build_silver_df(bronze_df)
    gold_df = build_gold_df(silver_df, run_id)
    quality_df, execution_df, quality_results = build_quality_dfs(
        spark,
        bronze_df,
        silver_df,
        gold_df,
        run_id,
    )

    write_bronze_df(bronze_df, paths["bronze"])
    write_silver_df(silver_df, paths["silver"])
    write_gold_df(gold_df, paths["gold"])
    write_ops_dfs(quality_df, execution_df, paths["quality"], paths["execution"])

    failed_critical_checks = find_failed_quality_checks(quality_results)
    if fail_on_critical_quality:
        enforce_quality_gate(quality_results)

    return {
        "bronze_output_path": str(paths["bronze"]),
        "silver_output_path": str(paths["silver"]),
        "gold_output_path": str(paths["gold"]),
        "quality_results_path": str(paths["quality"]),
        "execution_events_path": str(paths["execution"]),
        "source_record_count": len(source_records),
        "silver_record_count": silver_df.count(),
        "gold_record_count": gold_df.count(),
        "quality_gate_status": "fail" if failed_critical_checks else "pass",
        "critical_quality_failure_count": len(failed_critical_checks),
        "run_id": run_id,
    }
