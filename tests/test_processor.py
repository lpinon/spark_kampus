import chispa

import main.config.constants as Constants
import main.processors.processor as Processor


class TestProcessor(object):

    def test_group_visits_by_video(self, spark):
        source_data = [
            (1234, 11111),
            (1234, 22222),
            (5678, 33333),
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            [Constants.VISITS_VIDEO_ID, Constants.VISITS_LOCATION_ID]
        )
        actual_df = Processor.group_visits_by_video(source_df)
        expected_data = [
            (1234, 2),
            (5678, 1)
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, Constants.VISITSXVIDEO_COUNT]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    def test_add_video_info(self, spark):
        source_data = [
            (1234, 2),
            (5678, 1),
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, Constants.VISITSXVIDEO_COUNT]
        )

        source_ref_data = [
            (1234, "Hello"),
            (5678, "All ok!"),
        ]
        source_ref_df = spark.spark_session.createDataFrame(
            source_ref_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, "somecol"]
        )

        actual_df = Processor.add_video_info(source_df, source_ref_df)
        expected_data = [
            (1234, 2, "Hello"),
            (5678, 1, "All ok!"),
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            [Constants.VISITSXVIDEO_VIDEO_ID, Constants.VISITSXVIDEO_COUNT, "somecol"]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)
