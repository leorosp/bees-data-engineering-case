REQUIRED_BREWERY_FIELDS = (
    "brewery_id",
    "name",
    "brewery_type",
    "city",
    "country",
)

SILVER_COLUMNS = (
    "brewery_id",
    "name",
    "brewery_type",
    "city",
    "state_province",
    "country",
    "postal_code",
    "street",
    "website_url",
    "phone",
    "longitude",
    "latitude",
    "landing_date",
    "run_id",
)

GOLD_COLUMNS = (
    "brewery_type",
    "country",
    "state_province",
    "brewery_count",
    "run_id",
    "generated_at_utc",
)
