import datetime
import json
import time

from progressbar import ProgressBar, Bar, Percentage
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType

from simulator import random_generator
from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.connectors.postgresql_connector import PostgreSQLConnector
from simulator.visit_generator import VisitGenerator

from kafka import KafkaProducer

KAFKA_TOPIC = 'visits'
CLIENT_ID = 'server.01'

kafka_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    client_id=CLIENT_ID,
    max_request_size=2147483647
)


def main():
    config = {
        "spark.jars.packages": "org.postgresql:postgresql:9.4.1211",
        "spark.driver.memory": "8g",
        Constants.DELTA_SRC_PATH: Constants.DELTA_LOCATION,
        Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
        Constants.POSTGRESQL_HOST: Constants.POSTGRESQL_HOST_VALUE,
        Constants.KAFKA_SERVER: Constants.KAFKA_SERVER_NAME
    }
    spark_configuration = SparkConfiguration(app_name="visits_events_generator", spark_master="local[2]",
                                             log_level="INFO", configuration=config)
    postgres_driver = PostgreSQLConnector(spark_configuration)

    videos_df = postgres_driver.get_table('video')
    users_df = postgres_driver.get_table('visitors')
    devices_df = postgres_driver.get_table('device')
    locations_df = postgres_driver.get_table('locations')

    visits_generator = VisitGenerator(videos_df, users_df, devices_df, locations_df)

    spark_configuration.spark_session.stop()
    while True:
        sleep_time = input("Sleep ms (default 10ms):")
        sleep_time = int(sleep_time) if sleep_time != '' else 10
        generated_visits = visits_generator.generate_visits()
        loops = input("Number of loops (default 1):")
        loops = int(loops) if loops != '' else 1
        for _ in range(loops):
            for i in range(len(generated_visits)):
                generated_visits[i]["visit_date"] = datetime.datetime.now()
                value_to_send = json.dumps(generated_visits[i], default=str).encode('utf-8')
                print(value_to_send)
                kafka_producer.send(KAFKA_TOPIC, value=value_to_send)
                time.sleep(sleep_time / 1000)


if __name__ == "__main__":
    main()
