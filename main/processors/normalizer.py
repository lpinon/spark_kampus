from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col, concat, lit
import main.config.constants as Constants


def add_visit_id(df_visit: DataFrame) -> DataFrame:
    return df_visit.withColumn(Constants.VISITS_ID, concat(
        col(Constants.VISITS_USER_ID), lit("_"),
        col(Constants.VISITS_VIDEO_ID), lit("_"),
        col(Constants.VISITS_DEVICE_ID), lit("_"),
        col(Constants.VISITS_LOCATION_ID), lit("_"),
        col(Constants.VISITS_VISIT_TIMESTAMP)
    ))


def normalize_visit(df_visit: DataFrame) -> DataFrame:
    normalized_visit = add_visit_id(df_visit)
    return normalized_visit
