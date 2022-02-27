from typing import Dict, Optional

from pyspark import SparkContext
from pyspark.sql import SparkSession

from .configuration_provider import ConfigurationProvider


class SparkSessionManager:
    def __init__(
        self,
        configuration_provider: ConfigurationProvider,
        session_config: Dict[str, str] = {},
    ) -> None:
        self.__spark: Optional[SparkSession] = None
        self.__config_provider = configuration_provider
        self.__session_config = session_config
        self.__initialized = False

    def initialize(self) -> None:
        # initialized session with additional config
        self.__initialized = True
        builder = self.__get_base_builder()

        for key, value in self.__session_config.items():
            builder.config(key, value)

        return builder.getOrCreate()

    def __get_base_builder(self) -> SparkSession.Builder:
        # returns builder with base configuration
        app_name = self.__config_provider.get("app_name")
        builder = (
            SparkSession.Builder().master("local").appName(app_name).enableHiveSupport()
        )
        return builder

    def get_new_session(self) -> None:
        if not self.__initialized:
            raise SparkNotInitializedError(
                "You must run `initialize` before getting a new session."
            )

        current_sc = self.get_current_context()

        builder = self.__get_base_builder()
        builder.config(current_sc.getConf())

        return builder.getOrCreate()

    def get_current_context(self) -> SparkContext:
        return self.__spark.sparkContext


class SparkNotInitializedError(Exception):
    pass
