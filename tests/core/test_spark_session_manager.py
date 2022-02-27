import pytest
from snek_case.core import JsonConfigurationProvider, SparkSessionManager

# TODO


class TestConfigurationProvider(JsonConfigurationProvider):
    def __init__(self) -> None:
        config_file = "/workspaces/template-pyspark/snek_case/config/config.json"
        super().__init__(config_file)


def test_can_create() -> None:
    # Assemble
    config = TestConfigurationProvider()

    # Act / Assert
    SparkSessionManager(config)


def test_can_initialize() -> None:
    # Assemble
    config = TestConfigurationProvider()
    spark = SparkSessionManager(config)

    # Act / Assert
    spark.initialize()


@pytest.mark.skip("Needs to run independant")
def test_can_add_additional_config() -> None:
    # Assemble
    config = TestConfigurationProvider()
    additional_config = {"spark.app.name": "test_app"}

    # Act
    spark_manager = SparkSessionManager(config, additional_config)
    spark = spark_manager.initialize()

    # Assert
    assert spark.sparkContext.appName == "test_app"
