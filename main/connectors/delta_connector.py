from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.streaming import DataStreamReader

from main.config.spark_config import SparkConfiguration
from pyspark.sql.utils import AnalysisException
from main.exceptions.ConfigNotFoundError import ConfigNotFoundError
import main.config.constants as Constants

# noinspection PyUnresolvedReferences
from delta.tables import DeltaTable


class DeltaConnector:
    def __init__(self, config: SparkConfiguration):
        self.spark_configuration = config
        self.delta_src = self.spark_configuration.get_config(Constants.DELTA_SRC_PATH)
        if not self.delta_src:
            raise ConfigNotFoundError

    def get_stream(self, table: str) -> DataStreamReader:
        return self.spark_configuration.spark_session.readStream.format("delta") \
            .option("ignoreChanges", "true") \
            .load(self.delta_src + table)
        # .option("ignoreChanges", "true")\

    def update_sum_count_or_insert(self, new_data: DataFrame, table: str, id_col: str):
        try:
            delta_table = DeltaTable.forPath(self.spark_configuration.spark_session, self.delta_src + table)
        except AnalysisException:
            # If delta table not exists just create it
            new_data.write \
                .format("delta") \
                .save(self.delta_src + table)
            return
        delta_table.alias("current_data").merge(
            new_data.alias("updates"),
            "current_data.{0} = updates.{0}".format(id_col))\
            .whenMatchedUpdate(set={
                "count": "current_data.count + updates.count"
            }) \
            .whenNotMatchedInsertAll() \
            .execute()
