from pyspark.sql import SparkSession
import os
from main.config.constants import *
from main.exceptions.ConfigNotFoundError import ConfigNotFoundError


class SparkConfiguration:
    def __init__(self, app_name="my_pyspark_app", spark_master="local[*]", log_level="INFO",
                 parallelism=4, data_partitions=2, configuration={}):
        # GET ENV VARIABLE
        level = os.getenv('LOG_LEVEL')
        self.log_level = level if level is not None else log_level
        self.spark_master = spark_master
        self.app_name = app_name
        spark_config = SparkSession.builder.master(self.spark_master).appName(self.app_name)
        spark_config.config("spark.default.parallelism", parallelism) \
            .config("spark.sql.shuffle.partitions", data_partitions)
        for key, value in configuration.items():
            spark_config.config(key, value)
        self.spark_session = spark_config.getOrCreate()
        self.spark_session.sparkContext.setLogLevel(self.log_level)

    def get_config(self, key: str) -> str:
        try:
            return self.spark_session.sparkContext.getConf().get(key)
        except Exception:
            raise ConfigNotFoundError()
