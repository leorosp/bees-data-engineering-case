import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from bees_case.pyspark_local import create_spark_session, run_local_pyspark_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the BEES case locally or in Google Colab with PySpark."
    )
    parser.add_argument(
        "--source-file",
        default="examples/sample_breweries.json",
        help="Path to the source JSON file with brewery records.",
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
        default=datetime.now(timezone.utc).strftime("local-pyspark-%Y%m%d%H%M%S"),
        help="Execution identifier for the local run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    try:
        summary = run_local_pyspark_pipeline(
            spark=spark,
            source_path=Path(args.source_file),
            output_root=Path(args.output_dir),
            landing_date=args.landing_date,
            run_id=args.run_id,
        )
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
