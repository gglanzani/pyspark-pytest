import pytest

import findspark
findspark.init()

import pyspark.sql.functions as sf  # more import if needed here
import my_package.my_file as mf

from fixtures import spark_context, hive_context

pytestmark = pytest.mark.usefixtures("spark_context", "hive_context")

def test_define_df(spark_context, hive_context):
    df = mf.define_df(hive_context).coalesce(1)
    row, = df.select(sf.max('age')).collect()
    assert row['max(age)'] == 2


def test_filter_str(hive_context):
    df = mf.define_df(hive_context).coalesce(1)
    filtered = mf.filter_str(df, 'name', 'bar')
    row, = filtered.collect()
    assert row['name'] == 'foo'
