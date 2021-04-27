from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.exceptions.ConfigNotFoundError import ConfigNotFoundError
from pyspark.sql.streaming import DataStreamReader


class KafkaConnector:
    def __init__(self, config: SparkConfiguration):
        self.config = config
        self.kafka_server = config.get_config(Constants.KAFKA_SERVER)
        if not self.kafka_server:
            raise ConfigNotFoundError()

    def get_stream(self, topic: str) -> DataStreamReader:
        return self.config.spark_session.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server).option("subscribe", topic)
