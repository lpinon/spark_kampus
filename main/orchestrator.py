import main.connectors.mock_database_connector as DatabaseConnector
import main.processors.normalizer as Normalizer
from main.connectors.delta_connector import DeltaConnector
import main.processors.processor as Processor

from pyspark.sql.functions import col


def ingest(new_data, spark_configuration):
    pass
