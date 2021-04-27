import psycopg2

from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, broadcast, coalesce


class PostgreSQLConnector:
    def __init__(self, config: SparkConfiguration):
        self.config = config
        self.database = config.get_config(Constants.POSTGRESQL_DB)
        self.host = config.get_config(Constants.POSTGRESQL_HOST)
        self.username = config.get_config(Constants.POSTGRESQL_USER)
        self.password = config.get_config(Constants.POSTGRESQL_PASSWORD)
        self.jdbc = "jdbc:postgresql://{0}/{1}?user={2}&password={3}".format(self.host, self.database, self.username,
                                                                             self.password)

    def get_table(self, table: str) -> DataFrame:
        return self.config.spark_session.read.format('jdbc').options(
            url=self.jdbc,
            dbtable=table,
            driver="org.postgresql.Driver"
        ).load()

    def store(self, data: DataFrame, table: str, mode="append"):
        data.write.mode(mode).jdbc(
            url=self.jdbc,
            table=table,
            mode=mode,
            properties={
                "driver": "org.postgresql.Driver"
            }
        )

    def store_only_new_records(self, df: DataFrame, table: str, id_col: str):
        new_data = df.select(col(id_col)).withColumn("needs_insert", lit(True))
        existing_data = self.get_table(table).select(col(id_col)).withColumn("is_new", lit(False))
        data_to_insert = existing_data \
            .join(broadcast(new_data), [id_col], "right_outer") \
            .withColumn("to_insert", coalesce(col("is_new"), col("needs_insert"))) \
            .filter(col("to_insert").equalTo(lit(True))) \
            .select(col(id_col)) \
            .distinct()
        new_dataframe_to_store = df.join(broadcast(data_to_insert), [id_col], "right_outer")
        self.store(new_dataframe_to_store, table)

    def delete_in(self, table: str, id_col: str, ids_to_delete: list):
        ids_query = ", ".join(["'{}'".format(el) for el in ids_to_delete])
        if len(ids_query) > 0:
            connection = psycopg2.connect(dbname=self.database, user=self.username, password=self.password,
                                          host=self.host)
            with connection:
                with connection.cursor() as curs:
                    curs.execute("DELETE FROM {0} WHERE {1} IN {2}".format(table, id_col, ids_query))
            # leaving contexts doesn't close the connection
            connection.close()

    def update_or_append(self, df: DataFrame, table: str, id_col: str):
        ids_to_delete = [el[0] for el in df.select(id_col).distinct().collect()]
        self.delete_in(table, id_col, ids_to_delete)
        self.store(df, table)
