from typing import Dict

from pyspark import SparkContext
from pyspark.sql import SparkSession

from .configuration_provider import ConfigurationProvider


class SparkSessionManager:
    def __init__(
        self,
        configuration_provider: ConfigurationProvider,
        session_config: Dict[str, str] = {},
    ) -> None:
        self.__config_provider = configuration_provider
        self.__session_config = session_config
        self.__initialized = False

    def initialize(self) -> SparkSession:
        # initialized session with additional config
        self.__initialized = True
        builder = self.__get_base_builder()

        for key, value in self.__session_config.items():
            builder.config(key, value)

        self.__spark = builder.getOrCreate()

        return self.__spark

    def __get_base_builder(self) -> SparkSession.Builder:
        # returns builder with base configuration
        app_name = self.__config_provider.get("app_name")
        builder = (
            SparkSession.Builder().master("local").appName(app_name).enableHiveSupport()
        )
        return builder

    def get_current_context(self) -> SparkContext:
        if not self.__initialized:
            raise SparkNotInitializedError()

        return self.__spark.getActiveSession().sparkContext


class SparkNotInitializedError(Exception):
    pass
