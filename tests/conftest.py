import pytest
from main.config.sparksession import ss


@pytest.fixture(scope='session')
def spark():
    return ss
