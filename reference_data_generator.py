from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType

import random_generator
from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.kafka_connector import KafkaConnector
from main.connectors.postgresql_connector import PostgreSQLConnector


def main():
    config = {
        "spark.jars.packages": "org.postgresql:postgresql:9.4.1211",
        Constants.CURRENT_DATA_DELTA_TABLE_NAME: Constants.CURRENT_DATA,
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME
    }
    spark_configuration = SparkConfiguration(app_name="reference_data_generation", spark_master="local[*]",
                                             log_level="INFO", configuration=config)

    visitors_data = random_generator.generate_random_users(10 ** 6)
    rdd_visitors = spark_configuration.spark_session.sparkContext.parallelize([
        (us["username"], us["email"], us["birth_date"], us["gender"], us["phone_number"], us["id_country"])
        for us in visitors_data
    ])
    schema_visitor = StructType([
        StructField('username', StringType(), False),
        StructField('email', StringType(), False),
        StructField('birth_date', TimestampType(), False),
        StructField('gender', StringType(), False),
        StructField('phone_number', StringType(), True),
        StructField('id_country', IntegerType(), False)
    ])
    # Create data frame
    visitors_df = spark_configuration.spark_session.createDataFrame(rdd_visitors, schema_visitor)
    PostgreSQLConnector(spark_configuration).store(visitors_df, "visitors")

    videos_data = random_generator.generate_random_videos(10 ** 5)
    videos_rdd = spark_configuration.spark_session.sparkContext.parallelize([
         (vid["id_user"], vid["title"], vid["id_language"], vid["category"])
         for vid in videos_data
     ])
    schema_videos = StructType([
        StructField('id_user', IntegerType(), False),
        StructField('title', StringType(), False),
        StructField('id_language', IntegerType(), False),
        StructField('category', StringType(), False),
    ])
    # Create data frame
    videos_df = spark_configuration.spark_session.createDataFrame(videos_rdd, schema_videos)
    PostgreSQLConnector(spark_configuration).store(videos_df, "video")


if __name__ == "__main__":
    main()
