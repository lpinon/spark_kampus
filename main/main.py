from config.sparksession import sc, ss, DeltaTable
from pyspark.sql.functions import lit, col, current_timestamp


def main():
    data = ss.range(1, 5000)
    data = data.withColumn("somedata", lit("a"))
    data = data.withColumn("ts", current_timestamp())
    data.write.format("delta").mode("overwrite").partitionBy(['somedata']).save("delta/delta_sample")

    new_data = ss.range(3000, 10000)
    new_data = new_data.withColumn("somedata", lit("b"))
    new_data = new_data.withColumn("ts", current_timestamp())
    new_data.write.format("delta").mode("overwrite").partitionBy(['somedata']).save("delta/delta_sample2")

    delta_b = DeltaTable.forPath(ss, "delta/delta_sample2")
    delta_b.delete("id >= 8000")
    delta_b.update(condition="id > 6500", set={"somedata": "'c'"})

    delta_a = DeltaTable.forPath(ss, "delta/delta_sample")
    delta_a.alias("delta_merge").merge(
        delta_b.toDF().alias("updates"),
        "delta_merge.id = updates.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    a_df = delta_a.toDF().cache()
    b_df = delta_b.toDF().cache()
    a_df.select(col("somedata")).distinct().show()
    b_df.select(col("somedata")).distinct().show()
    a_df.show()
    b_df.show()
    delta_a.history().show(10, False)
    # data.vacuum(200)  # vacuum files not required by versions more than 200 hours old
    # data.history().show(10, False)


if __name__ == "__main__":
    main()
