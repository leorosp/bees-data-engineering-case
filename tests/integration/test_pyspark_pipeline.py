import json

import pytest

from bees_case.contracts import GOLD_COLUMNS, SILVER_COLUMNS
from bees_case.api_runner import run_api_pyspark_pipeline
from bees_case.pyspark_local import (
    build_bronze_df,
    build_gold_df,
    build_quality_dfs,
    build_silver_df,
    run_local_pyspark_pipeline,
)
from bees_case.quality import QualityGateError


def _sample_records() -> list[dict]:
    return [
        {
            "id": "brew-1",
            "name": "Sample Brewery A",
            "brewery_type": "micro",
            "street": "1 Main St",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3030000001",
            "website_url": "https://example-a.com",
        },
        {
            "id": "brew-1",
            "name": "Sample Brewery A Duplicate",
            "brewery_type": "micro",
            "street": "1 Main St",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3030000001",
            "website_url": "https://example-a.com",
        },
        {
            "id": "brew-2",
            "name": "Sample Brewery B",
            "brewery_type": "brewpub",
            "street": "2 Oak Ave",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80203",
            "country": "United States",
            "longitude": "-104.9800",
            "latitude": "39.7400",
            "phone": "3030000002",
            "website_url": "https://example-b.com",
        },
    ]


def _clean_records() -> list[dict]:
    return [
        {
            "id": "brew-1",
            "name": "Sample Brewery A",
            "brewery_type": "micro",
            "street": "1 Main St",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3030000001",
            "website_url": "https://example-a.com",
        },
        {
            "id": "brew-2",
            "name": "Sample Brewery B",
            "brewery_type": "brewpub",
            "street": "2 Oak Ave",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80203",
            "country": "United States",
            "longitude": "-104.9800",
            "latitude": "39.7400",
            "phone": "3030000002",
            "website_url": "https://example-b.com",
        },
        {
            "id": "brew-3",
            "name": "Sample Brewery C",
            "brewery_type": "micro",
            "street": "3 Pine Rd",
            "city": "Austin",
            "state_province": "Texas",
            "postal_code": "73301",
            "country": "United States",
            "longitude": "-97.7431",
            "latitude": "30.2672",
            "phone": "5120000003",
            "website_url": "https://example-c.com",
        },
    ]


def _bad_records() -> list[dict]:
    return [
        {
            "id": "brew-1",
            "name": "",
            "brewery_type": "micro",
            "street": "1 Main St",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3030000001",
            "website_url": "https://example-a.com",
        },
        {
            "id": "brew-1",
            "name": "Sample Brewery A Duplicate",
            "brewery_type": "micro",
            "street": "1 Main St",
            "city": "Denver",
            "state_province": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3030000001",
            "website_url": "https://example-a.com",
        },
    ]


def test_build_silver_df_deduplicates_and_respects_contract(spark) -> None:
    bronze_df = build_bronze_df(
        spark=spark,
        source_records=_sample_records(),
        landing_date="2026-03-16",
        run_id="run-001",
    )

    silver_df = build_silver_df(bronze_df)

    assert silver_df.columns == list(SILVER_COLUMNS)
    assert silver_df.count() == 2
    assert silver_df.filter(silver_df.brewery_id == "brew-1").count() == 1


def test_build_gold_df_aggregates_by_type_and_location(spark) -> None:
    bronze_df = build_bronze_df(
        spark=spark,
        source_records=_sample_records(),
        landing_date="2026-03-16",
        run_id="run-001",
    )
    silver_df = build_silver_df(bronze_df)

    gold_df = build_gold_df(silver_df, run_id="run-001")

    assert gold_df.columns == list(GOLD_COLUMNS)
    assert gold_df.count() == 2
    micro_row = gold_df.filter(gold_df.brewery_type == "micro").collect()[0]
    assert micro_row["brewery_count"] == 1
    assert micro_row["country"] == "United States"


def test_build_quality_dfs_flags_required_fields_and_duplicates(spark) -> None:
    bronze_df = build_bronze_df(
        spark=spark,
        source_records=_bad_records(),
        landing_date="2026-03-16",
        run_id="bad-run",
    )
    silver_df = build_silver_df(bronze_df)
    gold_df = build_gold_df(silver_df, run_id="bad-run")

    quality_df, execution_df, quality_results = build_quality_dfs(
        spark=spark,
        bronze_df=bronze_df,
        silver_df=silver_df,
        gold_df=gold_df,
        run_id="bad-run",
    )

    assert quality_df.filter(quality_df.check_name == "required_fields").collect()[0][
        "status"
    ] == "fail"
    assert quality_df.filter(
        quality_df.check_name == "duplicate_primary_keys"
    ).collect()[0]["status"] == "fail"
    assert execution_df.collect()[0]["status"] == "failed"
    assert len(quality_results) == 3


def test_run_local_pyspark_pipeline_writes_expected_outputs(spark, tmp_path) -> None:
    source_path = tmp_path / "sample_records.json"
    source_path.write_text(json.dumps(_clean_records()), encoding="utf-8")

    summary = run_local_pyspark_pipeline(
        spark=spark,
        source_path=source_path,
        output_root=tmp_path / "artifacts",
        landing_date="2026-03-16",
        run_id="smoke-run",
    )

    assert summary["quality_gate_status"] == "pass"
    assert summary["source_type"] == "sample"
    assert summary["fallback_used"] is False
    assert (tmp_path / "artifacts" / "gold" / "breweries_by_type_location").exists()
    assert (tmp_path / "artifacts" / "ops" / "quality_results").exists()
    assert (tmp_path / "artifacts" / "ops" / "execution_events").exists()
    assert any(
        path.name.startswith("country=")
        for path in (tmp_path / "artifacts" / "silver" / "breweries").iterdir()
    )


def test_run_local_pyspark_pipeline_enforces_quality_gate(spark, tmp_path) -> None:
    source_path = tmp_path / "bad_records.json"
    source_path.write_text(json.dumps(_bad_records()), encoding="utf-8")

    with pytest.raises(QualityGateError, match="duplicate_primary_keys"):
        run_local_pyspark_pipeline(
            spark=spark,
            source_path=source_path,
            output_root=tmp_path / "bad_artifacts",
            landing_date="2026-03-16",
            run_id="bad-gate-run",
            fail_on_critical_quality=True,
        )


def test_run_api_pyspark_pipeline_records_source_provenance(spark, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "bees_case.api_runner.fetch_all_pages",
        lambda config: type(
            "ApiResult",
            (),
            {
                "records": _clean_records(),
                "pages_requested": 2,
                "pages_with_data": 2,
                "records_fetched": len(_clean_records()),
            },
        )(),
    )

    summary = run_api_pyspark_pipeline(
        spark=spark,
        output_root=tmp_path / "api_artifacts",
        landing_date="2026-03-16",
        run_id="api-run-001",
        fallback_to_sample=False,
        max_pages=2,
    )

    execution_df = spark.read.parquet(str(tmp_path / "api_artifacts" / "ops" / "execution_events"))
    execution_row = execution_df.collect()[0]

    assert summary["source_type"] == "api"
    assert summary["fallback_used"] is False
    assert summary["pages_requested"] == 2
    assert execution_row["source_type"] == "api"
    assert execution_row["fallback_used"] is False
    assert execution_row["records_fetched"] == len(_clean_records())
