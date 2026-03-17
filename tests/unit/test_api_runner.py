from bees_case.api_runner import build_api_run_config, fetch_api_records


def test_build_api_run_config_uses_runtime_parameters() -> None:
    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        per_page=50,
        max_pages=3,
    )

    assert config.base_path == "local_output"
    assert config.landing_date == "2026-03-16"
    assert config.run_id == "api-run-001"
    assert config.per_page == 50
    assert config.max_pages == 3


def test_fetch_api_records_raises_when_api_returns_no_rows(monkeypatch) -> None:
    monkeypatch.setattr("bees_case.api_runner.fetch_all_pages", lambda config: [])
    config = build_api_run_config(
        output_root="local_output",
        landing_date="2026-03-16",
        run_id="api-run-001",
        source_api_base_url="https://api.openbrewerydb.org/v1/breweries",
        per_page=50,
        max_pages=3,
    )

    try:
        fetch_api_records(config)
    except ValueError as error:
        assert "Open Brewery DB API" in str(error)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("fetch_api_records should fail when the API returns no rows.")
