import pytest

from bees_case.quality import (
    QualityGateError,
    enforce_quality_gate,
    find_failed_quality_checks,
)


def test_find_failed_quality_checks_only_returns_critical_failures() -> None:
    quality_results = [
        {"check_name": "required_fields", "status": "fail"},
        {"check_name": "duplicate_primary_keys", "status": "fail"},
        {"check_name": "negative_brewery_count", "status": "pass"},
    ]

    failed_checks = find_failed_quality_checks(quality_results)

    assert [check["check_name"] for check in failed_checks] == [
        "required_fields",
        "duplicate_primary_keys",
    ]


def test_enforce_quality_gate_raises_for_critical_failures() -> None:
    with pytest.raises(QualityGateError, match="required_fields"):
        enforce_quality_gate(
            [
                {"check_name": "required_fields", "status": "fail"},
                {"check_name": "negative_brewery_count", "status": "pass"},
            ]
        )
