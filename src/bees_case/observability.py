from datetime import datetime, timezone


def build_execution_event(
    *,
    layer: str,
    stage: str,
    status: str,
    run_id: str,
    records_in: int = 0,
    records_out: int = 0,
    details: str = "",
    **metadata,
) -> dict:
    return {
        "layer": layer,
        "stage": stage,
        "status": status,
        "run_id": run_id,
        "records_in": records_in,
        "records_out": records_out,
        "details": details,
        "event_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        **metadata,
    }
