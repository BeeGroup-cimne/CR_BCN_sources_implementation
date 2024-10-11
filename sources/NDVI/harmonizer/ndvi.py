import hashlib
import json
import os

import numpy as np
import utils
from neo4j import GraphDatabase

import settings
import morph_kgc
import pandas as pd
from datetime import timedelta
from utils.hbase import save_to_hbase
import pickle
import rdflib
from helpers import df_to_formatted_json


def split_into_zones(df_building_ndvi):
    zones_dict = {}
    zones_dict['darker'], zones_dict['dark'], zones_dict['medium'], zones_dict['light'], zones_dict[
        'lighter'] = [], [], [], [], []

    for c in df_building_ndvi.columns:
        if c != 'reference':
            if pd.to_numeric(c.split('_')[1]) < 0:
                zones_dict['darker'].append(c)
            if 0 < pd.to_numeric(c.split('_')[1]) < 0.2:
                zones_dict['dark'].append(c)
            if 0.2 < pd.to_numeric(c.split('_')[1]) < 0.4:
                zones_dict['medium'].append(c)
            if 0.4 < pd.to_numeric(c.split('_')[1]) < 0.6:
                zones_dict['light'].append(c)
            if pd.to_numeric(c.split('_')[1]) > 0.6:
                zones_dict['lighter'].append(c)

    df_building_ndvi['darker'] = df_building_ndvi[zones_dict['darker']].sum(axis=1)
    df_building_ndvi['dark'] = df_building_ndvi[zones_dict['dark']].sum(axis=1)
    df_building_ndvi['medium'] = df_building_ndvi[zones_dict['medium']].sum(axis=1)
    df_building_ndvi['light'] = df_building_ndvi[zones_dict['light']].sum(axis=1)
    df_building_ndvi['lighter'] = df_building_ndvi[zones_dict['lighter']].sum(axis=1)

    df_building_ndvi.drop([x for x in df_building_ndvi.columns if x.startswith('HISTO_')], axis=1, inplace=True)
    return df_building_ndvi


def building_percentage_format(df_building_ndvi):
    df_building_ndvi['darker'] = df_building_ndvi['darker'].astype(int)
    df_building_ndvi['dark'] = df_building_ndvi['dark'].astype(int)
    df_building_ndvi['medium'] = df_building_ndvi['medium'].astype(int)
    df_building_ndvi['light'] = df_building_ndvi['light'].astype(int)
    df_building_ndvi['lighter'] = df_building_ndvi['lighter'].astype(int)
    df_building_ndvi.set_index('reference', inplace=True)
    row_sums = df_building_ndvi[['darker', 'dark', 'medium', 'light', 'lighter']].sum(axis=1)

    return df_building_ndvi.div(row_sums, axis=0) * 100


def row_to_pickle(row):
    selected_values = row[['darker', 'dark', 'medium', 'light', 'lighter']]
    return pickle.dumps(selected_values)


def harmonize_ndvi(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)

    morph_config = '\n[DataSource1]\nmappings:sources/NDVI/harmonizer/mapping.yaml\nfile_path: {d_file}\n'

    # estadistica zonal
    df_building_ndvi = pd.read_csv('data/NDVI/08900_buildings_ndvi.csv')

    # drop buildings columns
    df_building_ndvi.drop([x for x in df_building_ndvi.columns if not (x == 'reference' or x.startswith('HISTO_'))],
                          axis=1, inplace=True)

    df_building_ndvi = split_into_zones(df_building_ndvi)
    df_building_ndvi = building_percentage_format(df_building_ndvi).reset_index()

    freq = "P1Y"
    user = "CRBCN"

    df_building_ndvi["ndviId"] = df_building_ndvi['reference'].apply(lambda x: (x + '-ndvi').encode("utf-8"))
    df_building_ndvi["ndviId"] = df_building_ndvi['ndviId'].apply(lambda x: hashlib.sha256(x).hexdigest())

    # Number of rows per split
    rows_per_split = 10000

    # Number of splits
    num_splits = len(df_building_ndvi) // rows_per_split

    # Split the DataFrame into parts
    df_building_ndvi_splits = np.array_split(df_building_ndvi, num_splits)

    # Load to Neo4J
    for i, split in enumerate(df_building_ndvi_splits):
        print(f"Split {i + 1}:\n{split}\n")
        with open("sources/NDVI/harmonizer/temp.json", "w") as d_file:
            json.dump({"ndvi": df_to_formatted_json(df_building_ndvi_splits[i], sep=".")}, d_file)

        g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
        os.unlink("sources/NDVI/harmonizer/temp.json")
        neo = GraphDatabase.driver(**config['neo4j'])
        content = g_rdflib.serialize(format="ttl")
        content = content.replace('\\"', "&apos;")
        content = content.replace("'", "&apos;")
        with neo.session() as s:
            response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
            print(response.single())

    df_building_ndvi['date'] = pd.to_datetime("2022/1/1")
    df_building_ndvi['start'] = df_building_ndvi['date'].astype(int) // 10 ** 9
    df_building_ndvi["bucket"] = (df_building_ndvi['start'].apply(int) // settings.ts_buckets) % settings.buckets
    df_building_ndvi['end'] = (df_building_ndvi['date'] + timedelta(days=365)).astype(int) // 10 ** 9
    df_building_ndvi['isReal'] = True
    df_building_ndvi['isSerializable'] = True

    # Aplicar la función a cada fila y guardar en la columna "concatenated"
    df_building_ndvi['pickle'] = df_building_ndvi.apply(row_to_pickle, axis=1)

    hbase_conn = config['hbase_store_harmonized_data']
    ndvi_device_table = f"harmonized_online_Ndvi_100_SUM_{freq}_{user}"
    save_to_hbase(df_building_ndvi.to_dict(orient="records"), ndvi_device_table, hbase_conn,
                  [("info", ['end', 'isReal', 'isSerializable']), ("v", ['pickle'])],
                  row_fields=['bucket', 'ndviId', 'start'])
    ndvi_period_table = f"harmonized_batch_Ndvi_100_SUM_{freq}_{user}"
    save_to_hbase(df_building_ndvi.to_dict(orient="records"), ndvi_period_table, hbase_conn,
                  [("info", ['end', 'isReal', 'isSerializable']), ("v", ['pickle'])],
                  row_fields=['bucket', 'start', 'ndviId'])
