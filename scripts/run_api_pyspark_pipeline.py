import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from bees_case.api_runner import run_api_pyspark_pipeline
from bees_case.pyspark_local import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the BEES case against the Open Brewery DB API with PySpark."
    )
    parser.add_argument(
        "--source-mode",
        default="api",
        choices=["api", "sample", "auto"],
        help="Source strategy for the run: API-first, sample-only or automatic fallback.",
    )
    parser.add_argument(
        "--output-dir",
        default="local_output",
        help="Directory where the bronze/silver/gold/ops outputs will be written.",
    )
    parser.add_argument(
        "--landing-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical ingestion date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime("api-pyspark-%Y%m%d%H%M%S"),
        help="Execution identifier for the API-backed run.",
    )
    parser.add_argument(
        "--source-api-base-url",
        default="https://api.openbrewerydb.org/v1/breweries",
        help="Open Brewery DB base URL.",
    )
    parser.add_argument(
        "--sample-file",
        default="examples/sample_breweries.json",
        help="Deterministic sample file used for demo mode or fallback.",
    )
    parser.add_argument(
        "--no-fallback-to-sample",
        action="store_false",
        dest="fallback_to_sample",
        help="Disable the sample fallback if the API path is unavailable.",
    )
    parser.set_defaults(fallback_to_sample=True)
    parser.add_argument(
        "--per-page",
        default=200,
        type=int,
        help="Page size used when reading from the API.",
    )
    parser.add_argument(
        "--max-pages",
        default=25,
        type=int,
        help="Maximum number of pages fetched from the API.",
    )
    parser.add_argument(
        "--api-timeout-seconds",
        default=30,
        type=int,
        help="Timeout for each API request in seconds.",
    )
    parser.add_argument(
        "--api-request-retries",
        default=1,
        type=int,
        help="Number of retries for each API page request.",
    )
    parser.add_argument(
        "--fail-on-critical-quality",
        action="store_true",
        help="Return a non-zero exit when critical quality checks fail.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session("bees-case-api")
    try:
        summary = run_api_pyspark_pipeline(
            spark=spark,
            output_root=Path(args.output_dir),
            landing_date=args.landing_date,
            run_id=args.run_id,
            source_mode=args.source_mode,
            source_api_base_url=args.source_api_base_url,
            sample_file=args.sample_file,
            fallback_to_sample=args.fallback_to_sample,
            per_page=args.per_page,
            max_pages=args.max_pages,
            api_timeout_seconds=args.api_timeout_seconds,
            api_request_retries=args.api_request_retries,
            fail_on_critical_quality=args.fail_on_critical_quality,
        )
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
