from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence


CRITICAL_QUALITY_CHECKS = (
    "required_fields",
    "duplicate_primary_keys",
)


class QualityGateError(RuntimeError):
    """Raised when one or more critical quality checks fail."""


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


def find_failed_quality_checks(
    quality_results: Sequence[Mapping[str, object]],
    critical_checks: Sequence[str] = CRITICAL_QUALITY_CHECKS,
) -> list[Mapping[str, object]]:
    critical_check_names = set(critical_checks)
    return [
        quality_result
        for quality_result in quality_results
        if quality_result.get("status") == "fail"
        and quality_result.get("check_name") in critical_check_names
    ]


def enforce_quality_gate(
    quality_results: Sequence[Mapping[str, object]],
    critical_checks: Sequence[str] = CRITICAL_QUALITY_CHECKS,
) -> None:
    failed_checks = find_failed_quality_checks(quality_results, critical_checks)
    if not failed_checks:
        return

    failed_check_names = ", ".join(
        sorted(str(quality_result["check_name"]) for quality_result in failed_checks)
    )
    raise QualityGateError(
        f"Critical quality checks failed: {failed_check_names}"
    )
