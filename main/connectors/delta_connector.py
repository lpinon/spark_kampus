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
        self.delta_src = self.spark_configuration.get_config(Constants.DELTA_SRC)
        if not self.current_data_table_name or not self.delta_src:
            raise ConfigNotFoundError

    def _generate_test_data(self) -> DataFrame:
        return self.spark_configuration.spark_session.range(1, 5000) \
            .withColumn("somedata", lit("a")) \
            .withColumn("somederiveddata", col("id") * 13 % 8) \
            .withColumn("test_ts", current_timestamp())

    def _init_current_data(self):
        test_data = self._generate_test_data()
        test_data.write\
            .format("delta")\
            .mode("overwrite")\
            .partitionBy(['somedata'])\
            .save(self.delta_src + self.current_data_table_name)

    def get_current_data(self) -> DeltaTable:
        try:
            delta_output = DeltaTable.forPath(self.spark_configuration.spark_session, self.delta_src
                                              + self.current_data_table_name)
        except AnalysisException:
            print("Delta Table not exists -> creating")
            self._init_current_data()
            delta_output = DeltaTable.forPath(self.spark_configuration.spark_session, self.delta_src
                                              + self.current_data_table_name)
        return delta_output
