import pytest

pytest.importorskip("luigi")

from orchestration.luigi_pipeline import PipelineOrchestration


def test_pipeline_orchestration_exposes_summary_marker() -> None:
    task = PipelineOrchestration(
        source_mode="file",
        source_file="examples/sample_breweries.json",
        output_dir="luigi_output",
        landing_date="2026-03-16",
        run_id="luigi-run-001",
    )

    assert task.output().path.endswith("_orchestration/pipeline_summary.json")


def test_pipeline_orchestration_defaults_to_api_mode() -> None:
    task = PipelineOrchestration()

    assert task.source_mode == "api"
