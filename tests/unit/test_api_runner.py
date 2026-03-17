from bees_case.api_client import ApiFetchError, ApiFetchResult
from bees_case.api_runner import build_api_run_config, fetch_api_records, resolve_source_records


def test_build_api_run_config_uses_runtime_parameters() -> None:
    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_mode="api",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        sample_file="examples/sample_breweries.json",
        fallback_to_sample=True,
        per_page=50,
        max_pages=3,
        api_timeout_seconds=30,
        api_request_retries=1,
    )

    assert config.base_path == "local_output"
    assert config.landing_date == "2026-03-16"
    assert config.run_id == "api-run-001"
    assert config.source_mode == "api"
    assert config.sample_file == "examples/sample_breweries.json"
    assert config.fallback_to_sample is True
    assert config.per_page == 50
    assert config.max_pages == 3


def test_fetch_api_records_raises_when_api_returns_no_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "bees_case.api_runner.fetch_all_pages",
        lambda config: ApiFetchResult(records=[], pages_requested=1, pages_with_data=0, records_fetched=0),
    )
    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_mode="api",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        sample_file="examples/sample_breweries.json",
        fallback_to_sample=True,
        per_page=50,
        max_pages=3,
        api_timeout_seconds=30,
        api_request_retries=1,
    )

    try:
        fetch_api_records(config)
    except ApiFetchError as error:
        assert "Open Brewery DB API" in str(error)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("fetch_api_records should fail when the API returns no rows.")


def test_resolve_source_records_prefers_api_when_available(monkeypatch) -> None:
    monkeypatch.setattr(
        "bees_case.api_runner.fetch_all_pages",
        lambda config: ApiFetchResult(
            records=[{"id": "brew-1"}],
            pages_requested=2,
            pages_with_data=1,
            records_fetched=1,
        ),
    )
    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_mode="api",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        sample_file="examples/sample_breweries.json",
        fallback_to_sample=True,
        per_page=50,
        max_pages=3,
        api_timeout_seconds=30,
        api_request_retries=1,
    )

    source = resolve_source_records(config)

    assert source.source_type == "api"
    assert source.fallback_used is False
    assert source.records_fetched == 1
    assert source.pages_requested == 2


def test_resolve_source_records_falls_back_to_sample(monkeypatch) -> None:
    def raise_api_error(config):
        raise ApiFetchError("network failed", pages_requested=1, records_fetched=0)

    monkeypatch.setattr("bees_case.api_runner.fetch_api_records", raise_api_error)
    monkeypatch.setattr(
        "bees_case.api_runner.load_records",
        lambda source_path: [{"id": "brew-sample"}],
    )

    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_mode="api",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        sample_file="examples/sample_breweries.json",
        fallback_to_sample=True,
        per_page=50,
        max_pages=3,
        api_timeout_seconds=30,
        api_request_retries=1,
    )

    source = resolve_source_records(config)

    assert source.source_type == "sample"
    assert source.fallback_used is True
    assert source.fallback_reason == "api_request_failed"
    assert source.records_fetched == 1
