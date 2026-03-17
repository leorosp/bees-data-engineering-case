from bees_case.api_client import ApiFetchError, ApiFetchResult, fetch_all_pages
from bees_case.config import PipelineRunConfig


def test_fetch_all_pages_accumulates_records_until_empty(monkeypatch) -> None:
    pages = {
        1: [{"id": "brew-1"}, {"id": "brew-2"}],
        2: [{"id": "brew-3"}],
        3: [],
    }

    def fake_fetch_page_with_retry(*, base_url, page, per_page, timeout, retries):
        return pages[page]

    monkeypatch.setattr("bees_case.api_client.fetch_page_with_retry", fake_fetch_page_with_retry)

    result = fetch_all_pages(
        PipelineRunConfig(base_path="local_output", max_pages=5, per_page=50)
    )

    assert isinstance(result, ApiFetchResult)
    assert result.records_fetched == 3
    assert result.pages_requested == 3
    assert result.pages_with_data == 2


def test_fetch_all_pages_raises_api_fetch_error_with_progress(monkeypatch) -> None:
    def fake_fetch_page_with_retry(*, base_url, page, per_page, timeout, retries):
        raise ApiFetchError("boom", pages_requested=page, records_fetched=0)

    monkeypatch.setattr("bees_case.api_client.fetch_page_with_retry", fake_fetch_page_with_retry)

    try:
        fetch_all_pages(PipelineRunConfig(base_path="local_output", max_pages=2, per_page=50))
    except ApiFetchError as error:
        assert error.pages_requested == 1
        assert error.records_fetched == 0
    else:  # pragma: no cover - defensive branch
        raise AssertionError("fetch_all_pages should propagate ApiFetchError.")
