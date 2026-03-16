from bees_case.config import PipelineRunConfig


def test_build_paths_from_base_path() -> None:
    config = PipelineRunConfig(base_path="abfss://lake@storage.dfs.core.windows.net")
    paths = config.build_paths()

    assert paths.bronze.endswith("/bronze")
    assert paths.silver.endswith("/silver")
    assert paths.gold.endswith("/gold")
    assert paths.ops.endswith("/ops")
