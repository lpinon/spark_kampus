from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType

from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from main.exceptions.ConfigNotFoundError import ConfigNotFoundError
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql import DataFrame


def extract_json_data(source: DataFrame, schema: StructType) -> DataFrame:
    return source.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


class KafkaConnector:
    def __init__(self, config: SparkConfiguration):
        self.config = config
        self.kafka_server = config.get_config(Constants.KAFKA_SERVER)
        if not self.kafka_server:
            raise ConfigNotFoundError()

    def get_stream(self, topic: str, start_from_begining=False) -> DataStreamReader:
        stream = self.config.spark_session.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server).option("subscribe", topic)
        if start_from_begining:
            stream = stream.option("startingOffsets", "earliest")
        return stream
