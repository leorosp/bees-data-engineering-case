from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence


def summarize_required_field_gaps(
    records: Iterable[Mapping[str, object]], required_fields: Sequence[str]
) -> dict[str, int]:
    counters = {field_name: 0 for field_name in required_fields}
    for record in records:
        for field_name in required_fields:
            value = record.get(field_name)
            if value in (None, ""):
                counters[field_name] += 1
    return counters


def has_duplicate_primary_keys(primary_keys: Sequence[str]) -> bool:
    normalized_keys = [key for key in primary_keys if key]
    return len(normalized_keys) != len(set(normalized_keys))


def build_quality_result(
    *,
    layer: str,
    check_name: str,
    status: str,
    metric_name: str,
    metric_value: int,
    run_id: str,
    message: str,
) -> dict:
    return {
        "layer": layer,
        "check_name": check_name,
        "status": status,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "run_id": run_id,
        "message": message,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }
