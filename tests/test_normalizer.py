import chispa

import main.config.constants as Constants
import main.processors.processor as Processor
import main.processors.normalizer as Normalizer


class TestNormalizer(object):

    def test_normalize_visit(self, spark):
        source_data = [
            (1, 2, 3, 4, 5),
            (10, 20, 30, 40, 50),
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            [Constants.VISITS_USER_ID,
             Constants.VISITS_VIDEO_ID,
             Constants.VISITS_DEVICE_ID,
             Constants.VISITS_LOCATION_ID,
             Constants.VISITS_VISIT_TIMESTAMP]
        )
        actual_df = Normalizer.normalize_visit(source_df)
        expected_data = [
            (1, 2, 3, 4, 5, '1_2_3_4_5'),
            (10, 20, 30, 40, 50, '10_20_30_40_50'),
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            [Constants.VISITS_USER_ID,
             Constants.VISITS_VIDEO_ID,
             Constants.VISITS_DEVICE_ID,
             Constants.VISITS_LOCATION_ID,
             Constants.VISITS_VISIT_TIMESTAMP,
             Constants.VISITS_ID]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    def test_normalize_count_by_videos(self, spark):
        source_data = [
            (1234, 2),
            (1234, 3),
            (5678, 10),
            (5678, 1),
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, Constants.VISITSXVIDEO_COUNT]
        )

        actual_df = Normalizer.normalize_count_by_videos(source_df)
        expected_data = [
            (1234, 3),
            (5678, 10)
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, Constants.VISITSXVIDEO_COUNT]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)
