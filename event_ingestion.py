from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.kafka_connector import KafkaConnector, extract_json_data


def main():
    # Configure Spark Session
    config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0,"
                               "org.postgresql:postgresql:9.4.1211,"
                               "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,"
                               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.driver.memory": "8g",
        "spark.scheduler.mode": "FAIR",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME,
    }
    spark_configuration = SparkConfiguration(app_name="visits_ads_event_ingestion", spark_master="local[*]",
                                             log_level="WARN", configuration=config)
    import main.orchestrator as Orchestrator

    ########################
    # Visit events ingestion
    ########################

    visits_schema = StructType([
        StructField('id_user', IntegerType(), False),
        StructField('id_video', IntegerType(), False),
        StructField('id_device', IntegerType(), False),
        StructField('id_location', IntegerType(), False),
        StructField('visit_date', TimestampType(), True)
    ])
    visits_stream = KafkaConnector(spark_configuration).get_stream('visits', start_from_begining=False).load()
    visits_stream = extract_json_data(visits_stream, visits_schema)

    # For each micro-batch of visit events
    visits_stream.writeStream\
        .foreachBatch(lambda visits_batch, index: Orchestrator.ingest_visits(visits_batch, spark_configuration, index))\
        .start()

    # Await stream termination
    spark_configuration.spark_session.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
