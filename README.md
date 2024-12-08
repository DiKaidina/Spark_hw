# Spark_hw


## Project structure

    1. spark_app.py - the main file with all the functions and business logic
    2. tests.py - a separate file with tests performed. 
       The functions for testing are injected from spark_app.py
       
    3. prepare_weather_df.py - a separate script for processing 
       weather_dataset -> added geohash for further join

    4. weather_geohash - where weather_dataframe wuth geohash column is stored

    5. final_dataframe_parquet - where the final dataframe is stored in parquet format + partitioned
    6. final_dataframe_csv - where the final dataframe is stored in csv format (did it for myself to take a look what i got)

    7. restaurant_raw & weather_raw - source data from hw



## Run tests.py

To run tests in terminal type

```bash
  pytest tests.py
```

## Important info 
The task description says: "Consider the weather datasets you need to download after analyzing the restaurant dataset"

I analyzed restaurants_df by lat/lng columns and calculated min/max
values. All of the weather dataframes per each period inludes this 
range. 

After analyzing weather datasets I desided to use only the last 
dataset per each month and that is why:

    1. For each month all the data from one dataset to another 
        is filled for the same lat/lng, 
        but has different temperature for each day
    
    2. Consider the previous point, it sounds logical to save the 
     data of the last days for each month since it is better to
     have the newest data for the same lat/lng. 
     I saved 3 datasets in total, 1 for October, August, September, and then join this data to restaurants_df.

## I left the comments in the project files, but if smth is unclear, please write me directly
