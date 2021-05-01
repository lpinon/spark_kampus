from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.kafka_connector import KafkaConnector, extract_json_data
from main.connectors.postgresql_connector import PostgreSQLConnector


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
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME,
    }
    spark_configuration = SparkConfiguration(app_name="visits_video_processor", spark_master="local[2]",
                                             log_level="WARN", configuration=config)
    from main.connectors.delta_connector import DeltaConnector
    import main.orchestrator as Orchestrator

    videos_ref_df = PostgreSQLConnector(spark_configuration).get_table(Constants.VIDEOS_TABLE).cache()
    visits_video = DeltaConnector(spark_configuration).get_stream(Constants.VISITSXVIDEO_TABLE)

    # For each micro-batch of visit events
    visits_video.writeStream \
        .option("checkpointLocation", "checkpoint/visits_video") \
        .foreachBatch(lambda visits_video_batch, index: Orchestrator.ingest_video_visits(visits_video_batch,
                                                                                         spark_configuration,
                                                                                         videos_ref_df,
                                                                                         index
                                                                                         )) \
        .trigger(processingTime='30 seconds') \
        .start()

    # Await stream termination
    spark_configuration.spark_session.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
