import hashlib
import json
import os
import numpy as np
from hbase import save_to_hbase
from neo4j import GraphDatabase
import pandas as pd
import beelib

def harmonize_endesa(config, **kwargs):
    morph_config = 'mapping.yaml'
    df_original = pd.read_parquet('/home/mmartinez/Nextcloud/Beegroup/data/CR_BCN_modeling/weather_downscaling/Historical_ERA5Land/Predictions/prediction_202006-202007.parquet')
    df_original["weatherId"] = df_original['weatherStation'].apply(lambda x: (x + '-weather').encode("utf-8"))
    df_original["weatherId"] = df_original['weatherId'].apply(lambda x: hashlib.sha256(x).hexdigest())
    df_original[['latitude','longitude']] = df_original['weatherStation'].str.split('_', expand=True).astype(float)


    # Load to Neo4j
    df = df_original.copy()
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.drop_duplicates(subset=['weatherStation'],inplace=True,keep='first')
    documents = {"weather": df.to_dict(orient='records')}
    beelib.beetransformation.map_and_save(documents, morph_config, config)


    # Load to HBase
    freq = "P1M"
    user = "CRBCN"

    ts_buckets = 10000000
    buckets = 4

    df_original['relativeHumidity'] = df_original['relativeHumidity'] * 100
    df_original['start'] = df_original['time'].astype(int) // 10 ** 9
    df_original['end'] = df_original['time'].apply(lambda x: (x + pd.offsets.MonthEnd(0))).astype(int) // 10 ** 9
    df_original["bucket"] = (df_original['start'].apply(int) // ts_buckets) % buckets
    df_original['isReal'] = False

    hbase_conn = config['hbase_store_raw_data']
    weather_device_table = f"harmonized_online_DryBulbTemperatureDownScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['airTemperature'])],
                  row_fields=['bucket', 'weatherId', 'start'])

    weather_device_table = f"harmonized_online_RelativeHumidityDownScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['relativeHumidity'])],
                  row_fields=['bucket', 'weatherId',  'start'])


    weather_period_table = f"harmonized_batch_DryBulbTemperatureDownScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['airTemperature'])],
                  row_fields=['bucket', 'start',  'weatherId'])

    weather_period_table = f"harmonized_batch_RelativeHumidityDownScaling_100_SUM_{freq}_{user}"
    save_to_hbase(df_original.to_dict(orient="records"), weather_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['relativeHumidity'])],
                  row_fields=['bucket', 'start', 'weatherId'])

config = beelib.beeconfig.read_config('/media/mmartinez/WINDOWS/TFM/BEEGroup/config.json')
harmonize_endesa(config)