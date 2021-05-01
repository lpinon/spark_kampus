from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
import main.config.constants as Constants


def group_visits_by_video(visits_df: DataFrame) -> DataFrame:
    return visits_df.groupBy(col(Constants.VISITS_VIDEO_ID)).count().alias("count")


def add_video_info(df_with_video_id: DataFrame, videos_ref: DataFrame):
    return df_with_video_id.join(videos_ref, Constants.VIDEOS_ID)
