import hashlib
import json
import os
import numpy as np
from hbase import save_to_hbase
from neo4j import GraphDatabase
import pandas as pd
import beelib

def harmonize_endesa(config, **kwargs):
    # df_buildings = pd.DataFrame(data)
    
    morph_config = 'sources/Weather_Scaling_Up/mapping.yaml'
    df_original = pd.read_csv('/home/mmartinez/Nextcloud/Beegroup/Projects/ClimateReady-BCN/WP3-VulnerabilityMap/Weather Scaling Up/test_csv_output_7days.csv', dtype=object)
    df_original = df_original[:100]
    df_original["weatherId"] = df_original['weatherStation'].apply(lambda x: (x + '-weather').encode("utf-8"))
    df_original["weatherId"] = df_original['weatherId'].apply(lambda x: hashlib.sha256(x).hexdigest())
    df_original[['latitude','longitude']] = df_original['weatherStation'].str.split('_', expand=True).astype(float)

    df = df_original.copy()
    df.drop_duplicates(subset=['weatherStation'],inplace=True,keep='first')

    # Load to Neo4j
    # documents = {"weather": df.to_dict(orient='records')}
    # beelib.beetransformation.map_and_save(documents, morph_config, config)


    freq = "P1M"
    user = "CRBCN"

    ts_buckets = 10000000
    buckets = 4

    df_original['relativeHumidity'] = df_original['relativeHumidity'].astype(float) * 100
    df_original['airTemperature'] = df_original['airTemperature'].astype(float) 
    df_original['forecastingTime'] = pd.to_datetime(df_original['forecastingTime'], format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')
    df_original['time'] = pd.to_datetime(df_original['time'], format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')
    df_original['start'] = df_original['time'].astype(int) // 10 ** 9
    df_original['end'] = df_original['time'].apply(lambda x: (x + pd.offsets.MonthEnd(0))).astype(int) // 10 ** 9
    df_original["bucket"] = (df_original['start'].apply(int) // ts_buckets) % buckets
    df_original['isReal'] = False

    # # Hbase
    hbase_conn = config['hbase_store_raw_data']
    weather_device_table = f"harmonized_online_DryBulbTemperatureScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['airTemperature'])],
                  row_fields=['bucket', 'weatherId', 'start'])

    weather_device_table = f"harmonized_online_RelativeHumidityScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['relativeHumidity'])],
                  row_fields=['bucket', 'weatherId', 'start'])


    weather_period_table = f"harmonized_batch_DryBulbTemperatureScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['airTemperature'])],
                  row_fields=['bucket', 'start', 'weatherId'])

    weather_period_table = f"harmonized_batch_RelativeHumidityScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['relativeHumidity'])],
                  row_fields=['bucket', 'start', 'weatherId'])

config = beelib.beeconfig.read_config('/media/mmartinez/WINDOWS/TFM/BEEGroup/config.json')
harmonize_endesa(config)