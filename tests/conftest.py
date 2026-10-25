import pytest

from gas_analytics.config import Config
from gas_analytics.runtime import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark(Config(spark_master="local[2]", shuffle_partitions=2))
    yield session
    session.stop()
