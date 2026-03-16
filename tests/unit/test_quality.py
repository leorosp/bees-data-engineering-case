from bees_case.quality import has_duplicate_primary_keys, summarize_required_field_gaps


def test_summarize_required_field_gaps_counts_missing_values() -> None:
    records = [
        {"brewery_id": "1", "name": "Alpha", "country": "US"},
        {"brewery_id": "", "name": "Beta", "country": ""},
    ]

    result = summarize_required_field_gaps(records, ("brewery_id", "country"))

    assert result["brewery_id"] == 1
    assert result["country"] == 1


def test_has_duplicate_primary_keys_detects_duplicates() -> None:
    assert has_duplicate_primary_keys(["1", "2", "1"]) is True
    assert has_duplicate_primary_keys(["1", "2", "3"]) is False
