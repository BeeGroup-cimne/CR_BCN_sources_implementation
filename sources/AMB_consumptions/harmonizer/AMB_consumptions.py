import hashlib
import json
import os
from datetime import timedelta

import morph_kgc
import pandas as pd
import utils
from neo4j import GraphDatabase
from utils.hbase import save_to_hbase

import settings

config = utils.utils.read_config(settings.conf_file)

"""    
import json
import tempfile
import os
import morph_kgc
import pandas as pd
import rdflib
morph_config = '\n[DataSource1]\nmappings:data/AMB_consumptions/mapping.yaml\nfile_path: {d_file}\n'
json_data = json.load(open("data/AMB_consumptions/data.json"))

"""

def df_to_formatted_json(df, sep="."):
    """
    The opposite of json_normalize
    """
    result = []
    for idx, row in df.iterrows():
        parsed_row = {}
        for col_label,v in row.items():
            keys = col_label.split(sep)

            current = parsed_row
            for i, k in enumerate(keys):
                if i==len(keys)-1:
                    current[k] = v
                else:
                    if k not in current.keys():
                        current[k] = {}
                    current = current[k]
        result.append(parsed_row)
    return result

def harmonize_AMB_consumptions(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)

    buildings_df = pd.read_csv("data/Inspire/building/08900_buildings_geom.csv")
    buildings_df['reference'] = buildings_df['gml_id'].apply(lambda x: x.split('.')[-1])
    cat_ref_list = buildings_df.reference.to_list()

    df_consumptions = pd.read_excel('data/AMB_consumptions/Dades vulnerabilitat energètica habitatges_CIMNE.xlsx', engine='openpyxl')
    df_building_consumption = df_consumptions[df_consumptions.REFCAT.isin(cat_ref_list)]

    df_building_consumption.to_json('data/AMB_consumptions/data.json', orient='records')
    df_dict = {}
    df_dict['consumptions'] = json.loads(df_building_consumption.to_json(orient="records"))
    with open("data/AMB_consumptions/data.json", "w") as d_file_data:
        json.dump(df_dict, d_file_data)

    morph_config = '\n[DataSource1]\nmappings:data/AMB_consumptions/mapping.yaml\nfile_path: {d_file}\n'
    json_data = json.load(open("data/AMB_consumptions/data.json"))
    df_data_consumptions = pd.json_normalize(json_data['consumptions'])

    freq = "P1Y"
    user = "CRBCN"

    df_data_consumptions.dropna(subset=['Total gas (kWh/m²)', 'Total gas (kWh/m²)'], inplace=True)

    df_data_consumptions["gasDeviceId"] = df_data_consumptions['REFCAT'].apply(lambda x: (x + '-Gas').encode("utf-8"))
    df_data_consumptions["gasDeviceId"] = df_data_consumptions['gasDeviceId'].apply(lambda x: hashlib.sha256(x).hexdigest())

    df_data_consumptions["electricityDeviceId"] = df_data_consumptions['REFCAT'].apply(lambda x: (x + '-Electricity').encode("utf-8"))
    df_data_consumptions["electricityDeviceId"] = df_data_consumptions['electricityDeviceId'].apply(
        lambda x: hashlib.sha256(x).hexdigest())

    with open("sources/AMB_consumptions/harmonizer/temp.json", "w") as amb_file:
        json.dump({"consumptions": df_to_formatted_json(df_data_consumptions, sep=".")}, amb_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=amb_file.name))
    os.unlink("sources/AMB_consumptions/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())

    hbase_conn = config['hbase_store_harmonized_data']
    df_data_consumptions['date'] = pd.to_datetime("2017/1/1")
    df_data_consumptions['start'] = df_data_consumptions['date'].astype(int) // 10 ** 9
    df_data_consumptions["bucket"] = (df_data_consumptions['start'].apply(
        int) // settings.ts_buckets) % settings.buckets
    df_data_consumptions['end'] = (df_data_consumptions['date'] + timedelta(days=365)).astype(int) // 10 ** 9
    df_data_consumptions['isReal'] = True
    df_data_consumptions['gasValue'] = df_data_consumptions['Total gas (kWh/m²)'] * df_data_consumptions[
        'V306_SCT.Scons.total']
    df_data_consumptions['electricityValue'] = df_data_consumptions['Total electricitat (kWh/m²)'] * \
                                               df_data_consumptions['V306_SCT.Scons.total']

    electricity_device_table = f"harmonized_online_EnergyConsumptionGridElectricity_100_SUM_{freq}_{user}"
    save_to_hbase(df_data_consumptions.to_dict(orient="records"), electricity_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['electricityValue'])],
                  row_fields=['bucket', 'electricityDeviceId', 'start'])
    electricity_period_table = f"harmonized_batch_EnergyConsumptionGridElectricity_100_SUM_{freq}_{user}"
    save_to_hbase(df_data_consumptions.to_dict(orient="records"), electricity_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['electricityValue'])],
                  row_fields=['bucket', 'start', 'electricityDeviceId'])

    gas_device_table = f"harmonized_online_EnergyConsumptionGas_100_SUM_{freq}_{user}"
    save_to_hbase(df_data_consumptions.to_dict(orient="records"), gas_device_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['gasValue'])],
                  row_fields=['bucket', 'gasDeviceId', 'start'])
    gas_period_table = f"harmonized_batch_EnergyConsumptionGas_100_SUM_{freq}_{user}"
    save_to_hbase(df_data_consumptions.to_dict(orient="records"), gas_period_table, hbase_conn,
                  [("info", ['end', 'isReal']), ("v", ['gasValue'])],
                  row_fields=['bucket', 'start', 'gasDeviceId'])


