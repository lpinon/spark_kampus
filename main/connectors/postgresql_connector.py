from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants


class PostgreSQLConnector:
    def __init__(self, config: SparkConfiguration):
        self.config = config
        self.database = config.get_config(Constants.POSTGRESQL_DB)
        self.host = config.get_config(Constants.POSTGRESQL_HOST)
        self.username = config.get_config(Constants.POSTGRESQL_USER)
        self.password = config.get_config(Constants.POSTGRESQL_PASSWORD)
        self.jdbc = "jdbc:postgresql://{0}/{1}?user={2}&password={3}".format(self.host, self.database, self.username,
                                                                             self.password)

    def get_table(self, table: str):
        return self.config.spark_session.read.format('jdbc').options(
            url=self.jdbc,
            dbtable=table,
            driver="org.postgresql.Driver"
        ).load()
