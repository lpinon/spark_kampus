from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp, col
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

    def update_or_insert(self, new_data: DataFrame, table: str, id_col: str):
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
            "current_data.{0} = updates.{0}".format(id_col)) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
