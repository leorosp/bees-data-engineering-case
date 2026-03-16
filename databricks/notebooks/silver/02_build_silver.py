# Databricks notebook source
import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from bees_case.config import PipelineRunConfig


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

bronze_input_path = f"{paths.bronze}/landing_date={config.landing_date}"
silver_output_path = f"{paths.silver}/breweries"

brewery_schema = T.StructType(
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

bronze_df = spark.read.json(bronze_input_path)

silver_df = (
    bronze_df.withColumn("payload", F.from_json(F.col("raw_payload"), brewery_schema))
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

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .partitionBy("country", "state_province")
    .save(silver_output_path)
)

dbutils.notebook.exit(
    json.dumps(
        {
            "silver_output_path": silver_output_path,
            "run_id": config.run_id,
            "row_count": silver_df.count(),
        }
    )
)
