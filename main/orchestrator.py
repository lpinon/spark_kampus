from pyspark.sql import DataFrame
from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
import main.processors.processor as Processor
import main.connectors.mock_database_connector as DatabaseConnector
import main.processors.normalizer as Normalizer
from main.connectors.delta_connector import DeltaConnector

from pyspark.sql.functions import col

from main.connectors.postgresql_connector import PostgreSQLConnector


def ingest_visits(raw_visits: DataFrame, spark_configuration: SparkConfiguration, index=0):
    print("Ingesting visits, batch - {}".format(index))
    normalized_visits = Normalizer.normalize_visit(raw_visits)
    normalized_visits.show(truncate=False)
    visits_by_video = Processor.group_visits_by_video(normalized_visits)
    visits_by_video.show(truncate=False)
    DeltaConnector(spark_configuration).update_or_insert(visits_by_video,
                                                         Constants.VISITSXVIDEO_TABLE,
                                                         Constants.VISITSXVIDEO_VIDEO_ID)
    PostgreSQLConnector(spark_configuration).store(normalized_visits, Constants.VISITS_TABLE, mode="append")

