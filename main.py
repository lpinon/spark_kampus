from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants


def main():
    # Configure Spark Session
    config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0,org.postgresql:postgresql:9.4.1211",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        Constants.CURRENT_DATA_DELTA_TABLE_NAME: Constants.CURRENT_DATA,
        Constants.DELTA_SRC: Constants.DELTA_LOCATION,
        # Constants.POSTGRESQL_DB: Constants.POSTGRESQL_DB_VALUE,
        # Constants.POSTGRESQL_USER: Constants.POSTGRESQL_USER_VALUE,
        # Constants.POSTGRESQL_PASSWORD: Constants.POSTGRESQL_PASSWORD_VALUE,
    }
    spark_configuration = SparkConfiguration(app_name="New Data Ingest", spark_master="local[*]",
                                             log_level="INFO", configuration=config)

    import main.connectors.mock_database_connector as DatabaseConnector
    import main.orchestrator as Orchestrator

    # EXTRACT NEW DATA
    new_data = DatabaseConnector.get_new_data(spark_configuration=spark_configuration)
    # new_data = PostgreSQLConnector(spark_configuration).get_table('some_table')

    # INGEST NEW DATA
    Orchestrator.ingest(new_data=new_data, spark_configuration=spark_configuration)


if __name__ == "__main__":
    main()
