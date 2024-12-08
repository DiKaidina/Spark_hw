# Spark_hw

A brief description of what this project works

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
