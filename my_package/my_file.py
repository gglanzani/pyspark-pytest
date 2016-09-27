import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as sf


def define_df(hive_context):
    "Dummy function"
    data = [('foo', 1),
            ('bar', 2)]
    schema = ['name', 'age']
    df = hive_context.createDataFrame(data, schema=schema)
    return df


def filter_str(df, column, string):
    return df.filter(~(sf.regexp_extract(column, '({})'.format(string), 1) == string))



def main():
    sc = SparkContext()
    hc = HiveContext()


