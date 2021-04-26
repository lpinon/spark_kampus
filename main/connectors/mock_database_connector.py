from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from main.config.spark_config import SparkConfiguration


def get_new_data(spark_configuration: SparkConfiguration) -> DataFrame:
    return spark_configuration.spark_session.range(3000, 10000)\
        .withColumn("someotherdata", lit("b"))
