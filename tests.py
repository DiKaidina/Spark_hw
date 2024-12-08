from spark_app import replace_nulls, geo_coord, get_geohash, add_geohash
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from dotenv import load_dotenv
import pygeohash as pgh

load_dotenv()
API_KEY = os.getenv("API_KEY")

#using fixtures prepare test data and additional params
@pytest.fixture(scope="module")
def spark():
    """Создание и инициализация SparkSession."""
    spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    yield spark
    spark.stop()


#Prepare test_data 
@pytest.fixture()
def df_restaurants(spark):
    """Prepare schema and fill it with test data"""
    schema_restaurants = StructType([
        StructField("id", StringType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True),
        StructField("geohash", StringType(), True),
        
    ])

    data = [
        
        ("206158430215", 8, "The Green Olive", 53370, "US", "Haltom City", None, None, None),
        ("68719476763", 28, "The Spicy Pickle", 77517, "US", "Grayling", 32.7890, -97.2799, pgh.encode(32.7890, -97.2799, precision=4)),
        ("128849018936", 57, "The Yellow Submarine", 5679, "FR", "Paris", 44.6570, -84.7440, pgh.encode(32.7890, -84.7440, precision=4)),
        ("154618822657", 2, "Bella Cucina", 41484, "US", "Fort Pierce", None, None, None),
    ]

    return spark.createDataFrame(data, schema_restaurants)

#TEST 1 - Check if function replace_nulls works as intended
def test_replace_nulls(df_restaurants):
    # Check if there is any null column for lat or lng
    updated_df = replace_nulls(df_restaurants, API_KEY)
    null_check = updated_df.filter(col("lat").isNull() | col("lng").isNull()).count()

    # Using assert here to fail the test if there are still null values
    assert null_check == 0, "TEST_1 FAILED. THERE ARE STILL NULLS IN LAT/LONG COLUMNS"

#TEST 2 - Check if geohash is generated properly
def test_geohash(df_restaurants):
    df_geohash = add_geohash(df_restaurants)
    result = df_geohash.collect()

    # Проверяем, что геохэш корректно сгенерирован или остается None для строк с отсутствующими lat/lng
    for row in result:
        if row.lat is not None and row.lng is not None:
            correct_geohash = pgh.encode(row.lat, row.lng, precision=4)
            assert row.geohash == correct_geohash, f"Incorrect geohash for id {row.id}"
            assert len(row.geohash) == 4, f"Geohash length is {len(row.geohash)} instead of 4 for id {row.id}"
        else:
            # Для строк с None в lat или lng, проверяем, что geohash тоже None
            assert row.geohash is None, f"Geohash must be None for id {row.id} for empty lat/lng"


#TEST 3 - Check if final DataFrame has dublicates
def test_join(spark):
    final_df = spark.read.option("header", "true").parquet("./final_dataframe_parquet")
    id_count = final_df.select("id").distinct().count()
    total_count = final_df.count()

    assert id_count == total_count, f"Found duplicates in final_df. Total count: {total_count}, Unique id count: {id_count}"
    



