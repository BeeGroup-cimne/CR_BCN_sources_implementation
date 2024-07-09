import hashlib
import json
import os
import tempfile
from functools import partial

import numpy as np
import utils
from neo4j import GraphDatabase
import settings
import morph_kgc
import pandas as pd
from utils.hbase import save_to_hbase


def df_to_formatted_json(df, sep="."):
    """
    The opposite of json_normalize
    """
    result = []
    for idx, row in df.iterrows():
        parsed_row = {}
        for col_label, v in row.items():
            keys = col_label.split(sep)

            current = parsed_row
            for i, k in enumerate(keys):
                if i == len(keys) - 1:
                    current[k] = v
                else:
                    if k not in current.keys():
                        current[k] = {}
                    current = current[k]
        result.append(parsed_row)
    return result


def harmonize_endesa(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)

    morph_config = '\n[DataSource1]\nmappings:data/Endesa/mapping.yaml\nfile_path: {d_file}\n'

    df = pd.read_csv('sources/Endesa/harmonizer/Electricity2022-95.csv', dtype=object)

    df = df.rename(
        columns={f'CONSUMO_M{i}': pd.Timestamp(f'2023-{i:02d}-01').strftime('%Y-%m-%d') for i in range(1, 13)})

    df.drop(['Unnamed: 0', 'PROVINCE__C', 'PROVINCE_DESCRIPTION__C', 'BOROUGH__C',
             'BOROUGH_DESCRIPTION__C', 'Postal_Code__c', 'PROV'],
            inplace=True, axis=1)

    # Transpose date to rows
    df_melt = df.melt(
        id_vars=['STREET_TYPE__C', 'STREET_DESCRIPTION__C', 'STREET_NUMBER__C', 'NUM_CONTRATOS', 'address_x', 'matches',
                 'reference', 'CODPOS'],
        var_name='date',
        value_name='Consumo'
    )
    freq = "P1M"
    user = "CRBCN"

    df_melt.dropna(subset=['Consumo'], inplace=True)
    df_melt.dropna(subset=['reference'], inplace=True)
    df_melt["endesaId"] = df_melt['reference'].apply(lambda x: (x + '-endesa').encode("utf-8"))
    df_melt["endesaId"] = df_melt['endesaId'].apply(lambda x: hashlib.sha256(x).hexdigest())

    # Number of rows per split
    rows_per_split = 10000

    # Number of splits
    num_splits = len(df_melt) // rows_per_split

    # Split the DataFrame into parts
    df_melt_splits = np.array_split(df_melt, num_splits)

    # Load to Neo4J
    for i, split in enumerate(df_melt_splits):
        print(f"Split {i + 1}:\n{split}\n")
        with open("sources/Endesa/harmonizer/temp.json", "w") as d_file:
            json.dump({"endesa": df_to_formatted_json(df_melt_splits[i], sep=".")}, d_file)

        g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
        os.unlink("sources/Endesa/harmonizer/temp.json")
        neo = GraphDatabase.driver(**config['neo4j'])
        content = g_rdflib.serialize(format="ttl")
        content = content.replace('\\"', "&apos;")
        content = content.replace("'", "&apos;")
        with neo.session() as s:
            response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
            print(response.single())

    df_melt['date'] = pd.to_datetime(df_melt['date'], format='%Y-%m-%d', errors='coerce')
    df_melt['start'] = df_melt['date'].astype(int) // 10 ** 9
    df_melt['end'] = df_melt['date'].apply(lambda x: (x + pd.offsets.MonthEnd(0))).astype(int) // 10 ** 9
    df_melt["bucket"] = (df_melt['start'].apply(int) // settings.ts_buckets) % settings.buckets
    df_melt['isReal'] = True
    df_melt.rename(columns={'Consumo': 'value'}, inplace=True)

    # Hbase
    hbase_conn = config['hbase_store_harmonized_data']
    electricity_device_table = f"harmonized_online_EnergyConsumptionGridElectricityEndesa_100_SUM_{freq}_{user}"
    save_to_hbase(df_melt.to_dict(orient="records"), electricity_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['value'])],
                  row_fields=['bucket', 'endesaId', 'start'])
    electricity_period_table = f"harmonized_batch_EnergyConsumptionGridElectricityEndesa_100_SUM_{freq}_{user}"
    save_to_hbase(df_melt.to_dict(orient="records"), electricity_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['value'])],
                  row_fields=['bucket', 'start', 'endesaId'])
