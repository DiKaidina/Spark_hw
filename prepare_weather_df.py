from pyspark.sql import SparkSession
from spark_app import replace_nulls, geo_coord, get_geohash, add_geohash
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, when, udf
import pygeohash as pgh
import os
from dotenv import load_dotenv
import requests
from pyspark.sql import functions as F
import logging


spark = SparkSession.builder \
        .appName("weather_test") \
        .master("local[2]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.io.nativeio.enabled", "false")  \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

"""
The logic of datasets selection is described in README file

When reading weather pushdown filtering is performed to reduce the amount of input data.

The filter ranges are min/max  lat/lng from restaurants_df
"""
weather_raw_path = "./weather_raw/"
df_weather1 = spark.read.parquet(weather_raw_path) \
               .filter((F.col("lat") >= -25.437) & (F.col("lat") <= 58.3) &
               (F.col("lng") >= -159.481) & (F.col("lng") <= 115.164))

count = df_weather1.count()     
print(f"Weather count: {count}")  #8458560

df_weather1.show()

weather_with_geohash = add_geohash(df_weather1) 

weather_with_geohash.show()

"""Check if there is no nulls for geohash column""" 
#df_nulls = weather_with_geohash.filter(col("geohash").isNull()) 
#print("!!!!!!!!!! DF_NULLS COUNT !!!!!!!!!!!!!!!", df_nulls.count())

weather_with_geohash.write.parquet("./weather_geohash")

spark.stop()

