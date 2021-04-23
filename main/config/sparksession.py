from pyspark.sql import SparkSession

ss = (SparkSession.builder
      .master("local[4]")
      .appName("sparkapp")
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate())
# noinspection PyUnresolvedReferences
from delta.tables import DeltaTable
ss.sparkContext.setLogLevel("INFO")
sc = ss.sparkContext
