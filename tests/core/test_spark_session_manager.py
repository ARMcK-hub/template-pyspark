from snek_case.core import JsonConfigurationProvider, SparkSessionManager

# TODO


class TestConfigurationProvider(JsonConfigurationProvider):
    def __init__(self) -> None:
        config_file = "/workspaces/template-pyspark/snek_case/config/config.json"
        super().__init__(config_file)


def test_can_create() -> None:
    cp = TestConfigurationProvider()
    sm = SparkSessionManager(cp)

    sm.initialize()


def test_can_initialize() -> None:
    pass


def test_can_add_additional_config() -> None:
    pass


def test_can_get_new() -> None:
    pass


def test_new_matches_current_config() -> None:
    pass
