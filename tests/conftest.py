import pytest
from main.config.spark_config import SparkConfiguration


@pytest.fixture(scope='session')
def spark():
    return SparkConfiguration(app_name="Tests")
