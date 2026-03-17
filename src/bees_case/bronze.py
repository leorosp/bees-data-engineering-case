import json
from datetime import datetime, timezone
from typing import Iterable

from bees_case.api_client import build_api_url, fetch_all_pages as fetch_all_pages_result, fetch_page
from bees_case.config import PipelineRunConfig


def fetch_all_pages(config: PipelineRunConfig) -> list[dict]:
    return fetch_all_pages_result(config).records


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
