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
        self.current_data_table_name = self.spark_configuration.get_config(Constants.CURRENT_DATA_DELTA_TABLE_NAME)
        self.delta_src = self.spark_configuration.get_config(Constants.DELTA_SRC_PATH)
        if not self.current_data_table_name or not self.delta_src:
            raise ConfigNotFoundError
