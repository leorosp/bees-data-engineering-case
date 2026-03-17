import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java is required for PySpark tests.")

    try:
        from bees_case.pyspark_local import create_spark_session
    except ImportError as exc:
        pytest.skip(f"PySpark tests require the local extra dependencies: {exc}")

    spark_session = create_spark_session("bees-case-tests")
    yield spark_session
    spark_session.stop()
