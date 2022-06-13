import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import json_tuple, col, to_date, substring, from_unixtime


parser = argparse.ArgumentParser()
parser.add_argument("--date")
args = parser.parse_args()
dt = args.date

spark = SparkSession.builder.getOrCreate()

df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-headless.kafka.svc:9092") \
  .option("subscribe", "bithumb-ticker") \
  .option("includeHeaders", "true") \
  .load()

df.printSchema()

schema = StructType([
    StructField("acc_trade_value", DoubleType(), True),
    StructField("acc_trade_value_24H", DoubleType(), True),
    StructField("closing_price", DoubleType(), True),
    StructField("coin", StringType(), True),
    StructField("date", StringType(), True),
    StructField("fluctate_24H", DoubleType(), True),
    StructField("fluctate_rate_24H", DoubleType(), True),
    StructField("max_price", DoubleType(), True),
    StructField("min_price", DoubleType(), True),
    StructField("opening_price", DoubleType(), True),
    StructField("prev_closing_price", DoubleType(), True),
    StructField("units_traded", DoubleType(), True),
    StructField("units_traded_24H", DoubleType(), True)
])

out_df = df.select(json_tuple(col('value').cast('string'), "acc_trade_value", "acc_trade_value_24H", "closing_price", "coin",
                               "date", "fluctate_24H", "fluctate_rate_24H", "max_price", "min_price", "opening_price",
                               "prev_closing_price", "units_traded", "units_traded_24H")) \
           .toDF("acc_trade_value", "acc_trade_value_24H", "closing_price", "coin",
                 "date", "fluctate_24H", "fluctate_rate_24H", "max_price", "min_price", "opening_price",
                 "prev_closing_price", "units_traded", "units_traded_24H")

timestamp_col = substring(col('date'), 1, 10)
ts_string_col = from_unixtime(timestamp_col)

out_df = out_df.select(
    to_date(ts_string_col).alias('dt'),
    col('coin').cast('string'),
    ts_string_col.alias('date').cast('string'),
    timestamp_col.alias('timestamp').cast('string'),
    col('opening_price').cast('double'),
    col('closing_price').cast('double'),
    col('max_price').cast('double'),
    col('min_price').cast('double'),
    col('prev_closing_price').cast('double'),
    col('fluctate_24H').cast('double'),
    col('fluctate_rate_24H').cast('double'),
    col('acc_trade_value').cast('double'),
    col('acc_trade_value_24H').cast('double'),
    col('units_traded').cast('double'),
    col('units_traded_24H').cast('double'))

out_df = out_df.filter(col("dt") == dt)

query = out_df.repartition(10) \
    .write \
    .partitionBy("dt") \
    .mode("append") \
    .parquet("s3a://warehouse/raw/bithumb/ticker")
