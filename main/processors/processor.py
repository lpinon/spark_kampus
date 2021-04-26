from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


def process_new_data(new_data: DataFrame) -> DataFrame:
    return new_data\
        .withColumn("somederiveddata", col("id") * 13 % 8)\
        .where(col("id") < 8000)\
        .withColumn("somedata",
                    when(col("id") % 2 == 0, lit("c"))
                    .otherwise(col("somedata")))
