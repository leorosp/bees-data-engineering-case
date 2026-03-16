import json
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import urlopen

from bees_case.config import PipelineRunConfig


def build_api_url(base_url: str, page: int, per_page: int) -> str:
    query_string = urlencode({"page": page, "per_page": per_page})
    return f"{base_url}?{query_string}"


def fetch_page(base_url: str, page: int, per_page: int, timeout: int = 30) -> list[dict]:
    request_url = build_api_url(base_url, page, per_page)
    with urlopen(request_url, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def fetch_all_pages(config: PipelineRunConfig) -> list[dict]:
    all_records: list[dict] = []
    for page_number in range(1, config.max_pages + 1):
        page_records = fetch_page(
            base_url=config.source_api_base_url,
            page=page_number,
            per_page=config.per_page,
        )
        if not page_records:
            break
        all_records.extend(page_records)
    return all_records


def build_bronze_rows(
    records: Iterable[dict], landing_date: str, run_id: str
) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    output_rows: list[dict] = []
    for record in records:
        output_rows.append(
            {
                "record_id": str(record.get("id", "")),
                "landing_date": landing_date,
                "run_id": run_id,
                "ingested_at_utc": ingested_at,
                "raw_payload": json.dumps(record, sort_keys=True),
            }
        )
    return output_rows
