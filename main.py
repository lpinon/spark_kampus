from main.config.spark_config import SparkConfiguration
import main.config.constants as Constants


def main():
    # Configure Spark Session
    config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:0.8.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        Constants.CURRENT_DATA_DELTA_TABLE_NAME: Constants.CURRENT_DATA,
        Constants.DELTA_SRC: Constants.DELTA_LOCATION
    }
    spark_configuration = SparkConfiguration(app_name="New Data Ingest", spark_master="local[*]",
                                             log_level="INFO", configuration=config)

    import main.connectors.mock_database_connector as DatabaseConnector
    import main.orchestrator as Orchestrator

    # EXTRACT NEW DATA
    new_data = DatabaseConnector.get_new_data(spark_configuration=spark_configuration)

    # INGEST NEW DATA
    Orchestrator.ingest(new_data=new_data, spark_configuration=spark_configuration)


if __name__ == "__main__":
    main()
