from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, when, udf
from pyspark.sql import DataFrame
import pygeohash as pgh
import os
from dotenv import load_dotenv
import requests
from pyspark.sql import functions as F
import logging

def geo_coord(city, country, api_key):
    geo_url = "https://api.opencagedata.com/geocode/v1/json"
    query = f"{city}, {country}"
    
    params = {
        "q": query,
        "key": api_key,
        "limit": 1,
    }
    
    try:
        resp = requests.get(geo_url, params=params)
        resp.raise_for_status()
        data = resp.json()
        
        if data['results']:  # Check if results are not empty
            location = data['results'][0]['geometry']
            lat, lng = location['lat'], location['lng']
            return lat, lng
        else:
            return None, None
    except requests.exceptions.RequestException:
        return None, None

        
def replace_nulls(df, api_key):
    """
    Check for null values and replace them via OpenCage Geocoding API.
    """
    df_nulls = df.filter(col("lat").isNull() | col("lng").isNull()).collect()

    # Список новых координат
    new_coord = []

    for row in df_nulls:
        city = row['city']
        country = row['country']
        
        # Получаем новые координаты
        lat, lng = geo_coord(city, country, api_key)
        
        new_coord.append({
            "id": row["id"],
            "lat_new": lat,
            "lng_new": lng
        })
    
    # as Dataframe
    new_coord_df = df.sparkSession.createDataFrame(new_coord)

    # join with given df 
    df_with_coordinates = df.join(new_coord_df, on="id", how="left")

    # use coalesce to replace only where are new coordimnates
    df_with_coordinates = df_with_coordinates.withColumn(
        "lat", F.coalesce(col("lat_new"), col("lat"))
    ).withColumn(
        "lng", F.coalesce(col("lng_new"), col("lng"))
    )

    # drop extra columns
    df_with_coordinates = df_with_coordinates.drop("lat_new", "lng_new")

    return df_with_coordinates


def get_geohash(lat, lng):
    if lat is not None and lng is not None:
        return pgh.encode(lat, lng, precision=4)  
    return None

def add_geohash(df):
    # use UDF to apply function to each row in DataFrame
    geohash = udf(get_geohash, StringType())
    
    # add geohash to base DataFrame
    df_with_geohash = df.withColumn("geohash", geohash(df["lat"], df["lng"]))

    return df_with_geohash

def spark_join(df_1: DataFrame, df_2: DataFrame, join_condition, select_columns=None) -> DataFrame:
    """
    This function performs join of 2 DataFrames

    """
    res = df_1.join(df_2, join_condition, "left")

    
    # What columns we want in a result set 
    if select_columns:
        res = res.select(*select_columns)

    """
    Dedublication logic
    
    """

    # 1. Find maximum weather date for each id from restaurants DataFrame
    max_dates = (
        res.groupBy("id")
        .agg(F.max("wthr_date").alias("max_wthr_date"))
    )
    
    # 2. Leave only rows of max_wthr_date
    filtered_df = (
        res.join(max_dates, (res["id"] == max_dates["id"]) & (res["wthr_date"] == max_dates["max_wthr_date"]))
           .drop(max_dates["id"])  # Drop extra column 
    )

    
    
    # 3. Calculate avg temperature for rows. The rest columns remain the same, so I used "first" 
    final_df = (
        filtered_df.groupBy("id")
        .agg(
            F.first("franchise_id").alias("franchise_id"),
            F.first("franchise_name").alias("franchise_name"),
            F.first("restaurant_franchise_id").alias("restaurant_franchise_id"),
            F.first("country").alias("country"),
            F.first("city").alias("city"),
            F.first("lat").alias("lat"),  # Lat from restaurants
            F.first("lng").alias("lng"),  # Lng from restaurants
            F.first("geohash").alias("geohash"),
            F.first("lat_weather").alias("lat_weather"),  # Lat from weather
            F.first("lng_weather").alias("lng_weather"),  # Lng from weather
            F.round(F.avg("avg_tmpr_f"), 2).alias("avg_tmpr_f"),  # Avg_temp (F) for max_date
            F.round(F.avg("avg_tmpr_c"), 2).alias("avg_tmpr_c"),  # Avg_temp (C) for max_date
            F.first("wthr_date").alias("wthr_date"),  # Max weather_date 
            F.first("year").alias("year"),
            F.first("month").alias("month"),
            F.first("day").alias("day")
        )
    )
    

    return final_df



def main():
    # Create spark session and limit the allocated resources for spark
    spark = SparkSession.builder \
        .appName("Spark_hw") \
        .master("local[2]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.io.nativeio.enabled", "false")  \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #Applied schema manually instead of converting column types later in code
    schema_restaurants = StructType([
        StructField("id", StringType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True),
    ])

    #API_KEY is stored as env variable
    load_dotenv()
    api_key = os.getenv("API_KEY")

    #0. load data from csv / parquet
    restaurants_raw_path = "./restaurants_raw"
    weather_path = "./weather_geohash"
    df_restaurants = spark.read.option("header", "true").schema(schema_restaurants).csv(restaurants_raw_path)

    #Rename columns in weather,  not ot have [AMBIGUOUS_REFERENCE] error after join 
    df_weather = spark.read.parquet(weather_path)
    df_weather = df_weather.withColumnRenamed("lat", "lat_weather").withColumnRenamed("lng", "lng_weather").withColumnRenamed("geohash", "geohash_weather")

    #df_weather.show()
    #1. replace null values
    df_restaurants = replace_nulls(df_restaurants, api_key)

    #2. get geohash 
    df_restaurants_geohash = add_geohash(df_restaurants)

    #3. Left join restaurants and weather + perform dedublication 

    joined_df = spark_join(
                    df_restaurants_geohash, 
                    df_weather, 
                    join_condition = df_restaurants_geohash["geohash"] == df_weather["geohash_weather"]
            )

    #4. save additionally to csv format to look at DataFrame 
    
    #joined_df.write.option("header", "true").csv("./final_dataframe_csv") 

    #joined_df.write.partitionBy("wthr_date").format("parquet").save("./final_dataframe_parquet")

    spark.stop()

if __name__ == "__main__":
    main()
