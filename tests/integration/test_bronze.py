from bees_case.bronze import build_api_url, build_bronze_rows


def test_build_api_url_adds_page_and_page_size() -> None:
    url = build_api_url("https://api.openbrewerydb.org/v1/breweries", page=2, per_page=50)

    assert "page=2" in url
    assert "per_page=50" in url


def test_build_bronze_rows_preserves_payload_and_metadata() -> None:
    rows = build_bronze_rows(
        records=[{"id": "brew-1", "name": "Example Brewery"}],
        landing_date="2026-03-16",
        run_id="run-001",
    )

    assert rows[0]["record_id"] == "brew-1"
    assert rows[0]["landing_date"] == "2026-03-16"
    assert rows[0]["run_id"] == "run-001"
    assert "Example Brewery" in rows[0]["raw_payload"]
