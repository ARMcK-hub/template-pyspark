from typing import Dict

from pyspark import SparkContext
from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(
        self,
        app_name: str = "sparkApp",
        session_config: Dict[str, str] = {},
    ) -> None:
        self.__app_name = app_name
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
        builder = (
            SparkSession.Builder()
            .master("local")
            .appName(self.__app_name)
            .enableHiveSupport()
        )
        return builder

    def get_current_context(self) -> SparkContext:
        if not self.__initialized:
            raise SparkNotInitializedError()

        return self.__spark.getActiveSession().sparkContext


class SparkNotInitializedError(Exception):
    pass
