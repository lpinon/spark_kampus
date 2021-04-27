from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.kafka_connector import KafkaConnector
from main.connectors.postgresql_connector import PostgreSQLConnector


def main():
    # Configure Spark Session
    config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0,"
                               "org.postgresql:postgresql:9.4.1211,"
                               "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,"
                               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        Constants.CURRENT_DATA_DELTA_TABLE_NAME: Constants.CURRENT_DATA,
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME
    }
    spark_configuration = SparkConfiguration(app_name="visits_ads_event_ingestion", spark_master="local[*]",
                                             log_level="INFO", configuration=config)

    dataframe = PostgreSQLConnector(spark_configuration).get_table('video')
    dataframe.show(truncate=False)

    KafkaConnector(spark_configuration).get_stream('test').load()\
        .writeStream.format("console").outputMode("append").start().awaitTermination(30)


if __name__ == "__main__":
    main()
