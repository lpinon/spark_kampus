from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col, concat, lit
import main.config.constants as Constants
from pyspark.sql.functions import max as sparkMax

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


def normalize_count_by_videos(df_count_video: DataFrame) -> DataFrame:
    normalized_count_video = get_video_max_count_by_id(df_count_video,
                                                       Constants.VISITSXVIDEO_VIDEO_ID,
                                                       Constants.VISITSXVIDEO_COUNT)
    return normalized_count_video


def get_video_max_count_by_id(video_visits_raw: DataFrame, id_col: str, count_col: str):
    return video_visits_raw.groupBy(col(id_col)).agg(sparkMax(col(count_col))).withColumnRenamed("max(count)", "count")
