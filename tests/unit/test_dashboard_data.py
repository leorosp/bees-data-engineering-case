from pathlib import Path

from bees_case.dashboard_data import load_demo_dashboard_data


def test_load_demo_dashboard_data_reads_bundled_artifacts() -> None:
    demo_root = Path(__file__).resolve().parents[2] / "dashboard" / "demo_data"

    dashboard_data = load_demo_dashboard_data(demo_root)

    assert dashboard_data.source_mode == "demo"
    assert len(dashboard_data.gold) > 0
    assert len(dashboard_data.quality) > 0
    assert len(dashboard_data.execution) > 0
