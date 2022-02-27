import pytest
from snek_case.core import SparkSessionManager


def test_can_create() -> None:
    # Assemble / Act / Assert
    SparkSessionManager()


def test_can_initialize() -> None:
    # Assemble
    spark = SparkSessionManager()

    # Act / Assert
    spark.initialize()


@pytest.mark.skip("Needs to run independant")
def test_can_add_additional_config() -> None:
    # Assemble
    additional_config = {"spark.app.name": "test_app"}

    # Act
    spark_manager = SparkSessionManager(session_config=additional_config)
    spark = spark_manager.initialize()

    # Assert
    assert spark.sparkContext.appName == "test_app"
