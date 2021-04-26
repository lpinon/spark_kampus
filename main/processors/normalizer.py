from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def normalize_new_data(df_new_data: DataFrame):
    return df_new_data\
        .withColumnRenamed("someotherdata", "somedata_bad")\
        .withColumn("test_ts", current_timestamp())\
        .selectExpr("id", "somedata_bad AS somedata", "test_ts")
