#!/usr/bin/env python
# coding: utf-8

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date")
args = parser.parse_args()
dt = args.date

from pyspark import *
from pyspark.sql import SparkSession

import os
import socket

os.environ['PYSPARK_PYTHON'] = 'python3' # Needs to be explicitly provided as env. Otherwise workers run Python 2.7
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'  # Same

spark = SparkSession.builder.getOrCreate()

raw_df = spark.read.option("mode", "DROPMALFORMED").option("basePath", "s3a://coin-bucket/warehouse/raw/ticker").text(f's3a://coin-bucket/warehouse/raw/ticker')
print(dt)
raw_df.show()

raw_df = raw_df.filter(f"dt='{dt}'")
raw_df.groupby('dt').count().show()


from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
value_schema = StructType([ StructField('data', StringType()),
                            StructField('status', StringType())])
value_df = raw_df.withColumn('value_json', from_json('value', value_schema)).select(col('value_json.data').alias('data'), col('value_json.status').alias('status'))
value_df = value_df.filter('status = 0000').select('data')
value_df.cache()
value_df.show()

ticker_schema = StructType([StructField('acc_trade_value', StringType(), True),
                            StructField('acc_trade_value_24H', StringType(), True),
                            StructField('closing_price', StringType(), True),
                            StructField('fluctate_24H', StringType(), True),
                            StructField('fluctate_rate_24H', StringType(), True),
                            StructField('max_price', StringType(), True),
                            StructField('min_price', StringType(), True),
                            StructField('opening_price', StringType(), True),
                            StructField('prev_closing_price', StringType(), True),
                            StructField('units_traded', StringType(), True),
                            StructField('units_traded_24H', StringType(), True),
                            StructField('coin', StringType(), True),
                            StructField('timestamp', StringType(), True)])
ticker_array_schema = ArrayType(ticker_schema, True)


import json
from pyspark.sql.functions import udf, explode, from_unixtime, to_date
@udf(ticker_array_schema)
def parse_raw_ticker(data):
    data = json.loads(data)
    timestamp = data['date']
    del data['date']
    coins = list(data.keys())
    out = []
    for coin in coins:
        coin_item = data[coin]
        coin_item["coin"] = coin
        coin_item["timestamp"] = timestamp
        out.append(coin_item)
    return out

parsed_df = value_df.withColumn('parsed_arr', explode(parse_raw_ticker('data')))
parsed_df = parsed_df.select('parsed_arr.*')

parsed_df.show(10)

out_df = parsed_df.select(col('coin').cast(StringType()),
                         from_unixtime(col('timestamp')/1000).alias('timestamp'),
                         col('opening_price').cast(DoubleType()),
                         col('closing_price').cast(DoubleType()),
                         col('min_price').cast(DoubleType()),
                         col('max_price').cast(DoubleType()),
                         col('units_traded').cast(DoubleType()),
                         col('acc_trade_value').cast(DoubleType()),
                         col('prev_closing_price').cast(DoubleType()),
                         col('units_traded_24H').cast(DoubleType()),
                         col('acc_trade_value_24H').cast(DoubleType()),
                         col('fluctate_24H').cast(DoubleType()),
                         col('fluctate_rate_24H').cast(DoubleType()),
                         from_unixtime(col('timestamp')/1000, 'yyyy-MM-dd').alias('dt'))
try:
    loaded_df = spark.read.option("basePath", "s3a://coin-bucket/warehouse/data/ticker").parquet(f's3a://coin-bucket/warehouse/data/ticker/dt={dt}')

    out_df = loaded_df.union(out_df)
except:
    pass

out_df = out_df.distinct()
out_df.repartition('dt').write.partitionBy(['dt']).mode('overwrite').parquet('s3a://coin-bucket/warehouse/data/ticker')

spark.stop()
