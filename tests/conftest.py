import shutil

import pytest
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext


APP_NAME = 'pytest-pyspark-tests'


@pytest.fixture(scope="session", autouse=True)
def spark_context(request):
    """Fixture to create the SparkContext.

    The tests run on real data, so it should run on the cluster
    where the actual data is present.
    Args:
        request: pytest.FixtureRequest object
    Returns:
        HiveContext for tests
    """
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # request.addfinalizer(lambda: sc.stop())
    yield sc
    sc.stop()


@pytest.fixture(scope="session", autouse=True)
def hive_context(spark_context, request):
    """
    Fixture to create the HiveContext.

    Args:
        spark_context: spark_context fixture

    Returns:
    HiveContext
    """
    try:
        shutil.rmtree('metastore_db')
    except:
        pass
    # request.addfinalizer(lambda: spark_context.stop())
    yield HiveContext(spark_context)
