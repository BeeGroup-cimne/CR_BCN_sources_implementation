import copy
import json
import os
import re
import utils
from neo4j import GraphDatabase
from utils.hbase import save_to_hbase

import settings
import morph_kgc
import pandas as pd
import hashlib
from datetime import timedelta
import rdflib
from helpers import df_to_formatted_json

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


def datadis_taxonomies(df_melted):
    # fare, timeDiscrimination and economicSector to tariff
    taxonomy_df = pd.read_excel('sources/Datadis/harmonizer/DatadisTaxonomy.xlsx', sheet_name='tariff',
                                engine='openpyxl')
    tariff_dict = taxonomy_df.set_index('SOURCE')['Tariff'].to_dict()
    taxonomy_df.reset_index(inplace=True)
    # economic sector
    taxonomy_df['SOURCE'] = taxonomy_df['SOURCE'].apply(lambda x: x.split("~")[0])
    df_melted["tariff"] = df_melted["economicSector"] + "~" + df_melted["timeDiscrimination"] + "~" + df_melted["fare"]
    df_melted['tariff'] = df_melted['tariff'].apply(lambda x: tariff_dict[x])
    sector_dict = taxonomy_df.set_index('SOURCE')['EconomicSector'].to_dict()
    df_melted['economicSector'] = df_melted['economicSector'].apply(lambda x: sector_dict[x])
    return df_melted

def harmonize_datadis(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    df_buildings = pd.DataFrame(data)

    df_all = []
    for root, dirs, files in os.walk("data/Datadis/Postal_codes_hourly_electricity"):
        for fname in files:
            if re.match("^.*.csv$", fname):
                df = pd.read_csv(os.path.join(root, fname), sep="\t", index_col=0, dtype=str)
                df_all.append(df)
                del df

    df_all = pd.concat(df_all)
    unique_postal_codes = list(df_all.postalCode.unique())
    df_g = df_all.groupby("postalCode")
    del df_all
    tz_info = 'Europe/Madrid'
    df_all = []
    for postal_code in unique_postal_codes:
        # Select the postal code group
        df = copy.deepcopy(df_g.get_group(postal_code))
        df.postalCode = postal_code
        # Cast numeric columns to float
        for col in ['sumEnergy', 'sumContracts', 'mi1', 'mi2', 'mi3', 'mi4', 'mi5', 'mi6', 'mi7', 'mi8',
                    'mi9', 'mi10', 'mi11', 'mi12', 'mi13', 'mi14', 'mi15', 'mi16', 'mi17', 'mi18', 'mi19',
                    'mi20', 'mi21', 'mi22', 'mi23', 'mi24']:
            df[col] = df[col].astype(float)

        # Delete mi25
        df.drop('mi25', axis=1, inplace=True)

        df_melted = pd.melt(df, id_vars=['dataDay', 'dataMonth', 'dataYear', 'community', 'province',
                                         'municipality', 'postalCode', 'fare', 'timeDiscrimination',
                                         'measurePointType', 'sumEnergy', 'sumContracts', 'tension',
                                         'economicSector', 'distributor'], var_name='dataHour', value_name='value')

        df_melted["dataHour"] = df_melted['dataHour'].apply(
            lambda x: int(x.replace('mi', '')) - 1)
        df_melted["dataHour"] = df_melted['dataHour'].astype(str)
        # Add the date column

        df_melted["date"] = pd.to_datetime(
            dict(year=df_melted.dataYear, month=df_melted.dataMonth, day=df_melted.dataDay, hour=df_melted.dataHour),
            errors='coerce')
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2019') & (df_melted['dataMonth'] == '10') & (
                df_melted['dataDay'] == '27') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2019') & (df_melted['dataMonth'] == '3') & (
                df_melted['dataDay'] == '31') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2020') & (df_melted['dataMonth'] == '10') & (
                df_melted['dataDay'] == '25') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2020') & (df_melted['dataMonth'] == '3') & (
                df_melted['dataDay'] == '29') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2021') & (df_melted['dataMonth'] == '3') & (
                df_melted['dataDay'] == '28') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2021') & (df_melted['dataMonth'] == '10') & (
                df_melted['dataDay'] == '31') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2022') & (df_melted['dataMonth'] == '10') & (
                df_melted['dataDay'] == '30') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2022') & (df_melted['dataMonth'] == '3') & (
                df_melted['dataDay'] == '27') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2023') & (df_melted['dataMonth'] == '10') & (
                df_melted['dataDay'] == '29') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted.drop(df_melted[(df_melted['dataYear'] == '2023') & (df_melted['dataMonth'] == '3') & (
                df_melted['dataDay'] == '26') & (df_melted['dataHour'] == '2')].index, inplace=True)
        df_melted["start"] = df_melted['date'].dt.tz_localize(tz_info, ambiguous='infer')

        # Delete duplicates and drop unnecessary columns
        df_melted = df_melted[~df_melted.duplicated(
            subset=['municipality', 'distributor', 'measurePointType', 'postalCode', 'economicSector', 'fare',
                    'timeDiscrimination', 'start'], keep='last')]
        df_melted = df_melted.reset_index()
        df_melted = datadis_taxonomies(df_melted)

        df_melted_gb = df_melted.groupby(by=['postalCode', 'economicSector', 'tariff', 'start']).agg({"value": "sum"})
        df_melted_gb.reset_index(inplace=True)

        freq = "PT1H"
        user = "CRBCN"
        namespace = "http://bigg-project.eu/ld/ontology#"
        df_melted_gb["economicSectorUri"] = df_melted_gb['economicSector'].apply(
            lambda x: x.split('/'))
        df_melted_gb[
            'electricityDeviceId'] = df_melted_gb.postalCode + '-' + df_melted_gb.economicSector + '-' + df_melted_gb.tariff
        df_melted_gb["electricityDeviceId"] = df_melted_gb['electricityDeviceId'].apply(
            lambda x: (x + '-Electricity').encode("utf-8"))
        df_melted_gb["electricityDeviceId"] = df_melted_gb['electricityDeviceId'].apply(
            lambda x: hashlib.sha256(x).hexdigest())

        morph_config = '\n[DataSource1]\nmappings:sources/Datadis/harmonizer/mapping.yaml\nfile_path: {d_file}\n'
        with open("sources/Datadis/harmonizer/temp.json", "w") as amb_file:
            json.dump({"consumptions": df_to_formatted_json(
                df_melted_gb[['postalCode', 'economicSector', 'economicSectorUri', 'tariff', 'electricityDeviceId']], sep=".")}, amb_file)

        g_rdflib = morph_kgc.materialize(morph_config.format(d_file=amb_file.name))
        os.unlink("sources/Datadis/harmonizer/temp.json")
        neo = GraphDatabase.driver(**config['neo4j'])
        content = g_rdflib.serialize(format="ttl")
        content = content.replace('\\"', "&apos;")
        content = content.replace("'", "&apos;")

        with neo.session() as s:
            response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
            print(response.single())

        df_melted_gb['end'] = (df_melted_gb['start'] + timedelta(hours=1)).astype(int) // 10 ** 9
        df_melted_gb["start"] = df_melted_gb["start"].astype(int) // 10 ** 9
        df_melted_gb["bucket"] = (df_melted_gb['start'].apply(int) // settings.ts_buckets) % settings.buckets
        df_melted_gb['isReal'] = True

        # HBASE
        hbase_conn = config['hbase_store_harmonized_data']
        electricity_device_table = f"harmonized_online_EnergyConsumptionGridElectricityDatadis_100_SUM_{freq}_{user}"
        save_to_hbase(df_melted_gb.to_dict(orient="records"), electricity_device_table, hbase_conn,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'electricityDeviceId', 'start'])
        electricity_period_table = f"harmonized_batch_EnergyConsumptionGridElectricityDatadis_100_SUM_{freq}_{user}"
        save_to_hbase(df_melted_gb.to_dict(orient="records"), electricity_period_table, hbase_conn,
                      [("info", ['end', 'isReal']), ("v", ['value'])],
                      row_fields=['bucket', 'start', 'electricityDeviceId'])


