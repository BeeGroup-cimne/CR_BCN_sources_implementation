import json
import os
import utils
from neo4j import GraphDatabase

import settings
import morph_kgc
import pandas as pd
import re
import rdflib
from helpers import df_to_formatted_json

def get_address_street_name(df_point):

    address_street_names_df = pd.read_csv("data/Inspire/address/addressTh08015.csv", usecols = ['gml_id','text'])
    address_street_names_df["gml_id"] = address_street_names_df['gml_id'].apply(lambda x: x.split('ES.SDGC.TN.')[1])
    df_point["properties.merge_gml_id"] = df_point['properties.gml_id'].apply(lambda x: '.'.join(x.split('ES.SDGC.AD.')[1].split('.')[:3]))
    merge_df = pd.merge(df_point, address_street_names_df, left_on="properties.merge_gml_id", right_on="gml_id")
    merge_df.drop('gml_id', inplace=True, axis=1)
    merge_df.rename(columns={'text': 'properties.street_name'}, inplace=True)
    return merge_df

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def csv_to_json(name):
    df = pd.read_csv(f"""data/HUTS/{name}.csv""", index_col=[0])
    with open(f"""data/HUTS/{camel_to_snake(name)}_data.json""", "w") as d_file:
        json.dump({"HUTS": df_to_formatted_json(df, sep=".")}, d_file)

def harmonize_huts_static(data, huts_type, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)
    name = 'homesForTouristUseCP'
    # name = 'hotelsCP'
    huts_type = name

    morph_config = '\n[DataSource1]\nmappings:data/HUTS/mapping.yaml\nfile_path: {d_file}\n'
    json_data = json.load(open(f"""data/HUTS/{camel_to_snake(huts_type)}_data.json"""))
    huts_df = pd.json_normalize(json_data['HUTS'])
    huts_df['huts_type'] = "homesForTouristUse"
    # huts_df['huts_type'] = "hotels"

    with open("sources/HUTS/harmonizer/temp.json", "w") as d_file:
        json.dump({"huts": df_to_formatted_json(huts_df, sep=".")}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/HUTS/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())

    # df['listKey'] = measurement_id
    # device_table = f"harmonized_online_{data_type}_100_SUM_{freq}_{user}"
    #
    # save_to_hbase(df.to_dict(orient="records"),
    #               device_table,
    #               hbase_conn,
    #               [("info", ['end', 'isReal']), ("v", ['value'])],
    #               row_fields=['bucket', 'listKey', 'start'])

def harmonize_HUTS(data, **kwargs):
    # config = utils.utils.read_config(settings.conf_file)

    for name in ['homesForTouristUseCP']: #'homesForTouristUseCP' 'hotelsCP', 'hotels', 'homesForTouristUse',  'touristApartments'
        csv_to_json(name)
        harmonize_huts_static(data=data, huts_type=name)

