from __future__ import print_function, absolute_import

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as sf

from .util import get_schema


def define_df(hive_context):
    "Dummy function"
    data = [('foo', 1),
            ('bar', 2)]
    schema = get_schema()
    df = hive_context.createDataFrame(data, schema=schema)
    return df


def filter_str(df, column, string):
    return df.filter(~(sf.regexp_extract(column, '({})'.format(string), 1) == string))


def main(sc, hc):
    df = mf.define_df(hive_context).coalesce(1)
    filtered = mf.filter_str(df, 'name', 'bar')
    

if __name__ == "__main__":
    sc = SparkContext()
    hc = HiveContext()
    main(sc, hc)

