import shutil

import pytest
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext


APP_NAME = 'pytest-pyspark-tests'

def cleanup(spark_context):
    """
    On exit, kill the context.
    """
    spark_context.stop()


@pytest.fixture(scope="module")
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
    request.addfinalizer(lambda: sc.stop())
    return sc


@pytest.fixture(scope="module")
def hive_context(spark_context, request):
    """
    Fixture to create the HiveContext.

    Args:
        spark_context: spark_context fixture

    Returns:
    HiveContext
    """
    shutil.rmtree('metastore_db')
    request.addfinalizer(lambda: cleanup(spark_context))
    return HiveContext(spark_context)
