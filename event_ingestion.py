from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.postgresql_connector import PostgreSQLConnector
from main.connectors.kafka_connector import KafkaConnector


def foreach_batch_function(df, epoch_id):
    print(epoch_id)
    df.show(truncate=False)
    print(df.count())
    pass


def main():
    # Configure Spark Session
    config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0,"
                               "org.postgresql:postgresql:9.4.1211,"
                               "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,"
                               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.driver.memory": "8g",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        Constants.CURRENT_DATA_DELTA_TABLE_NAME: Constants.CURRENT_DATA,
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME,
    }
    spark_configuration = SparkConfiguration(app_name="visits_ads_event_ingestion", spark_master="local[*]",
                                             log_level="WARN", configuration=config)

    KafkaConnector(spark_configuration).get_stream('visits').load()\
        .writeStream.foreachBatch(foreach_batch_function).start()

    spark_configuration.spark_session.streams.awaitAnyTermination()
    #.format("console").outputMode("append").start().awaitTermination(30)


if __name__ == "__main__":
    main()
