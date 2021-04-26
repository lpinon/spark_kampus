import main.connectors.mock_database_connector as DatabaseConnector
import main.processors.normalizer as Normalizer
from main.connectors.delta_connector import DeltaConnector
import main.processors.processor as Processor

from pyspark.sql.functions import col


def ingest(new_data, spark_configuration):
    new_clean_data = Normalizer.normalize_new_data(new_data)
    new_processed_data = Processor.process_new_data(new_clean_data)
    # new_processed_data.show(truncate=False)

    delta = DeltaConnector(spark_configuration)
    current_data_delta = delta.get_current_data()
    # current_data_delta.toDF().show(truncate=False)

    current_data_delta.alias("current_data").merge(
        new_processed_data.alias("updates"),
        "current_data.id = updates.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    current_data_delta.toDF().show(truncate=False)
    current_data_delta.toDF().select(col("somedata")).distinct().show()
    current_data_delta.history().show(truncate=False)  # show delta change history
    # current_data_delta.vacuum(200)  # vacuum files not required by versions more than 200 hours old
