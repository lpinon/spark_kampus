from pyspark.sql.types import *

from tests.conftest import SparkConfiguration
import main.examples as T
from quinn.extensions import create_df
import chispa


class TestTransformations(object):

    def test_with_greeting(self, spark):
        source_data = [
            ("jose", 1),
            ("li", 2)
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            ["name", "age"]
        )
        actual_df = T.with_greeting(source_df)
        expected_data = [
            ("jose", 1, "hello!"),
            ("li", 2, "hello!")
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            ["name", "age", "greeting"]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    def test_with_greeting2(self, spark):
        source_data = [
            ("jose", 1),
            ("li", 2)
        ]
        source_df = spark.spark_session.createDataFrame(
            source_data,
            ["name", "age"]
        )
        actual_df = source_df.transform(T.with_greeting2("hi"))
        expected_data = [
            ("jose", 1, "hi"),
            ("li", 2, "hi")
        ]
        expected_df = spark.spark_session.createDataFrame(
            expected_data,
            ["name", "age", "greeting"]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    def test_with_clean_first_name(self, spark):
        source_df = spark.spark_session.create_df(
            [("jo&&se", "a"), ("##li", "b"), ("!!sam**", "c")],
            [("first_name", StringType(), True), ("letter", StringType(), True)]
        )
        actual_df = T.with_clean_first_name(source_df)
        expected_df = spark.spark_session.create_df(
            [("jo&&se", "a", "jose"), ("##li", "b", "li"), ("!!sam**", "c", "sam")],
            [("first_name", StringType(), True), ("letter", StringType(), True),
             ("clean_first_name", StringType(), True)]
        )
        chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)
