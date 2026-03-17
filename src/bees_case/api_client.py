import json
from dataclasses import dataclass
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bees_case.config import PipelineRunConfig


@dataclass(frozen=True)
class ApiFetchResult:
    records: list[dict]
    pages_requested: int
    pages_with_data: int
    records_fetched: int


class ApiFetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        pages_requested: int = 0,
        records_fetched: int = 0,
    ) -> None:
        super().__init__(message)
        self.pages_requested = pages_requested
        self.records_fetched = records_fetched


def build_api_url(base_url: str, page: int, per_page: int) -> str:
    query_string = urlencode({"page": page, "per_page": per_page})
    return f"{base_url}?{query_string}"


def fetch_page(base_url: str, page: int, per_page: int, timeout: int = 30) -> list[dict]:
    request_url = build_api_url(base_url, page, per_page)
    request = Request(
        request_url,
        headers={
            "User-Agent": "bees-data-engineering-case/0.1 (+https://github.com/leorosp/bees-data-engineering-case)",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def fetch_page_with_retry(
    *,
    base_url: str,
    page: int,
    per_page: int,
    timeout: int,
    retries: int,
) -> list[dict]:
    last_error: Exception | None = None
    for _ in range(retries + 1):
        try:
            return fetch_page(base_url=base_url, page=page, per_page=per_page, timeout=timeout)
        except Exception as error:  # pragma: no cover - exercised via wrapper behavior
            last_error = error
    raise ApiFetchError(
        f"Failed to fetch page {page} from the Open Brewery DB API: {last_error}",
        pages_requested=page,
        records_fetched=0,
    )


def fetch_all_pages(config: PipelineRunConfig) -> ApiFetchResult:
    all_records: list[dict] = []
    pages_with_data = 0
    pages_requested = 0

    for page_number in range(1, config.max_pages + 1):
        pages_requested = page_number
        try:
            page_records = fetch_page_with_retry(
                base_url=config.source_api_base_url,
                page=page_number,
                per_page=config.per_page,
                timeout=config.api_timeout_seconds,
                retries=config.api_request_retries,
            )
        except ApiFetchError as error:
            raise ApiFetchError(
                str(error),
                pages_requested=page_number,
                records_fetched=len(all_records),
            ) from error

        if not page_records:
            break
        pages_with_data += 1
        all_records.extend(page_records)

    return ApiFetchResult(
        records=all_records,
        pages_requested=pages_requested,
        pages_with_data=pages_with_data,
        records_fetched=len(all_records),
    )
