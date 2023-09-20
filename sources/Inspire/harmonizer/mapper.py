import hashlib
from collections import defaultdict
from datetime import datetime

import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

import settings
from sources.Inspire.harmonizer.Inspire_mapping import Mapping
from utils.data_transformations import *
from utils.hbase import save_to_hbase
from utils.neo4j import get_cups_id_link, get_device_from_datasource, create_sensor
from ontology.namespaces_definition import bigg_enums, units
from utils.rdf.rdf_functions import generate_rdf
from utils.rdf.save_rdf import save_rdf_with_source, link_devices_with_source
from utils.utils import log_string

bigg = settings.namespace_mappings['bigg']

def harmonize_data_ts(data, **kwargs):
    # Variables
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    tz_info = kwargs['timezone']

    # Database connections

    hbase_conn2 = config['hbase_store_harmonized_data']
    neo4j_connection = config['neo4j']
    neo = GraphDatabase.driver(**neo4j_connection)

    # Init dataframe
    df = pd.DataFrame.from_records(data)
    df = df.applymap(decode_hbase)
    # df = pd.read_excel('data/Generalitat Extracción_2018.xlsx',skiprows=2)

    # Add timezone
    df['Fecha fin Docu. cálculo'] = pd.to_datetime(df['Fecha fin Docu. cálculo'])
    df['Fecha fin Docu. cálculo'] += pd.Timedelta(hours=23)

    df['Fecha inicio Docu. cálculo'] = pd.to_datetime(df['Fecha inicio Docu. cálculo'])
    df['Fecha inicio Docu. cálculo'] = df['Fecha inicio Docu. cálculo'].dt.tz_localize(tz_info)
    df['Fecha fin Docu. cálculo'] = df['Fecha fin Docu. cálculo'].dt.tz_localize(tz_info)
    df['ts'] = df['Fecha inicio Docu. cálculo']
    # datatime64 [ns] to unix time
    df['start'] = df['Fecha inicio Docu. cálculo'].astype('int') / 10 ** 9
    df['start'] = df['start'].astype('int')

    df['end'] = df['Fecha fin Docu. cálculo'].astype('int') / 10 ** 9
    df['end'] = df['end'].astype('int')

    # Calculate kWh
    df['value'] = df['Consumo kWh ATR'].fillna(0).astype(float) + df['Consumo kWh GLP'].fillna(0).astype(float)

    # isReal
    df['isReal'] = df["Tipo Lectura"].map(defaultdict(lambda : False, {"ESTIMADA": False, "REAL": True}))

    # bucket
    df['bucket'] = (df['start'] // settings.ts_buckets) % settings.buckets

    df = df[['ts', 'bucket', 'CUPS', 'start', 'end', 'value', 'isReal']]

    for cups, data_group in df.groupby("CUPS"):
        data_group.set_index("ts", inplace=True)
        data_group.sort_index(inplace=True)

        dt_ini = data_group.iloc[0].name.tz_convert("UTC").tz_convert(None)
        dt_end = data_group.iloc[-1].name.tz_convert("UTC").tz_convert(None)

        with neo.session() as session:
            n = Namespace(namespace)
            devices_neo = list(get_device_from_datasource(session, user, cups, "NedgiaSource",
                                                          settings.namespace_mappings))
        for device in devices_neo:
            device_uri = device['d'].get("uri")
            sensor_id = sensor_subject("nedgia", cups, "EnergyConsumptionGas", "RAW", "")
            sensor_uri = str(n[sensor_id])
            measurement_id = hashlib.sha256(sensor_uri.encode("utf-8"))
            measurement_id = measurement_id.hexdigest()
            measurement_uri = str(n[measurement_id])
            with neo.session() as session:
                create_sensor(session, device_uri, sensor_uri, units["KiloW-HR"],
                              bigg_enums.EnergyConsumptionGas, bigg_enums.Naive,
                              measurement_uri, False,
                              False, False, "", "SUM", dt_ini, dt_end, settings.namespace_mappings)

            data_group['listKey'] = measurement_id
            device_table = f"harmonized_online_EnergyConsumptionGas_000_SUM__{user}"

            save_to_hbase(data_group.to_dict(orient="records"),
                          device_table,
                          hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'listKey', 'start'])
            period_table = f"harmonized_batch_EnergyConsumptionGas_000_SUM__{user}"
            save_to_hbase(data_group.to_dict(orient="records"),
                          period_table, hbase_conn2,
                          [("info", ['end', 'isReal']), ("v", ['value'])],
                          row_fields=['bucket', 'start', 'listKey'])


def prepare_df_clean_all(df):
    df['device_subject'] = df.CUPS.apply(partial(device_subject, source="nedgia"))
    df['utility_point_subject'] = df.utility_point_id.apply(delivery_subject)

def prepare_df_clean_linked(df):
    df['building_space_subject'] = df.NumEns.apply(building_space_subject)



def harmonize_data_device(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']

    neo = GraphDatabase.driver(**config['neo4j'])
    log_string("creating df", mongo=False)
    df = pd.DataFrame.from_records(data)
    log_string("preparing df", mongo=False)
    with neo.session() as session:
        nedgia_datasource = session.run(f"""
              MATCH (o:{bigg}__Organization{{userID:'{user}'}})-[:hasSource]->(s:NedgiaSource) return id(s)""").single()
        datasource = nedgia_datasource['id(s)']
    with neo.session() as session:
        device_id = get_cups_id_link(session, user, settings.namespace_mappings)
    df.loc[:, 'utility_point_id'] = df.CUPS.str[:20]
    df['NumEns'] = df.utility_point_id.map(device_id)
    df["source_id"] = datasource
    prepare_df_clean_all(df)
    linked_supplies = df[df["NumEns"].isna() == False]
    unlinked_supplies = df[df["NumEns"].isna()]

    for linked, df_t in [("linked", linked_supplies), ("unlinked", unlinked_supplies)]:
        if linked == "linked":
            prepare_df_clean_linked(df_t)
        n = Namespace(namespace)
        log_string("mapping df", mongo=False)
        mapping = Mapping(config['source'], n)
        g = generate_rdf(mapping.get_mappings(linked), df_t)
        log_string("saving df", mongo=False)
        save_rdf_with_source(g, config['source'], config['neo4j'])
    log_string("linking df", mongo=False)
    link_devices_with_source(df, n, config['neo4j'])

