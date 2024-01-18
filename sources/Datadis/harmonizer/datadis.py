import copy
import json
import os
import re
import tempfile
from functools import partial
import utils
from neo4j import GraphDatabase
from slugify import slugify
from utils.hbase import save_to_hbase

import settings
import morph_kgc
import pandas as pd
import hashlib
from datetime import timedelta
import rdflib
from utils.data_transformations import fuzzy_dictionary_match, fuzz_params

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

def harmonize_datadis(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    df_buildings = pd.DataFrame(data)

    # for postal_code in os.listdir("data/Datadis/Postal_codes_hourly_electricity"):
    #     if postal_code == "08033":
    #         for year_month in os.listdir("data/Datadis/Postal_codes_hourly_electricity/" + postal_code):
    #             if year_month == "08033":


    df_all = []
    for root, dirs, files in os.walk("data/Datadis/Postal_codes_hourly_electricity"):
        for fname in files:
            if re.match("^.*.csv$", fname):
                df = pd.read_csv(os.path.join(root, fname), sep="\t",
                                 index_col=0, dtype=str)
                df_all.append(df)
                del df

    df_all = pd.concat(df_all)
    unique_postal_codes = list(df_all.postalCode.unique())
    df_g = df_all.groupby("postalCode")
    del df_all
    for postal_code in unique_postal_codes:
        print(postal_code)
        # Select the postal code group
        df = copy.deepcopy(df_g.get_group(postal_code))
        df.postalCode = postal_code
        # Cast numeric columns to float
        for col in ['sumEnergy', 'sumContracts', 'mi1', 'mi2', 'mi3', 'mi4', 'mi5', 'mi6', 'mi7', 'mi8',
                    'mi9', 'mi10', 'mi11', 'mi12', 'mi13', 'mi14', 'mi15', 'mi16', 'mi17', 'mi18', 'mi19',
                    'mi20', 'mi21', 'mi22', 'mi23', 'mi24']:
            df[col] = df[col].astype(float)

        # Delete when mi25>0
        df = df[df.mi25 == "0"]

        df_melted = pd.melt(df, id_vars=['dataDay', 'dataMonth', 'dataYear', 'community', 'province',
       'municipality', 'postalCode', 'fare', 'timeDiscrimination',
       'measurePointType', 'sumEnergy', 'sumContracts', 'tension',
       'economicSector', 'distributor'], var_name='dataHour', value_name='Valor')

        df_melted["dataHour"] = df_melted['dataHour'].apply(
            lambda x: x.replace('mi', ''))

        # Add the date column
        df_melted["date"] = pd.to_datetime(dict(year=df_melted.dataYear, month=df_melted.dataMonth, day=df_melted.dataDay, hour=df_melted.dataHour))

        # Delete duplicates and drop unnecessary columns
        df_melted = df_melted[~df_melted.duplicated(
            subset=['municipality', 'distributor', 'measurePointType', 'postalCode', 'economicSector', 'fare',
                    'timeDiscrimination', 'date'], keep='last')]
        df_melted = df_melted.reset_index()

        # fare, timeDiscrimination and economicSector to tariff
        df_melted["tariff"] = df_melted["economicSector"] + "~" + df_melted["timeDiscrimination"] + "~" + df_melted["fare"]
        df_melted["tariff"] = df_melted["tariff"].map({
            "INDUSTRIA~GENERAL ALTA TENSIÓN~>= 1 kV Y < 30 kV": "3.1A",
            "INDUSTRIA~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
            "INDUSTRIA~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
            "INDUSTRIA~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
            "INDUSTRIA~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
            "INDUSTRIA~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
            "NO ESPECIFICADO~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
            "NO ESPECIFICADO~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
            "NO ESPECIFICADO~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
            "NO ESPECIFICADO~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHS",
            "NO ESPECIFICADO~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
            "NO ESPECIFICADO~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
            "RESIDENCIAL~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
            "RESIDENCIAL~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
            "RESIDENCIAL~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
            "RESIDENCIAL~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHS",
            "RESIDENCIAL~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
            "RESIDENCIAL~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
            "SERVICIOS~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
            "SERVICIOS~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
            "SERVICIOS~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
            "SERVICIOS~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
            "SERVICIOS~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A"
        })
        df_melted["economicSector"] = df_melted["economicSector"].map({
            'RESIDENCIAL': '1_residential',
            'INDUSTRIA': '2_agriculture + 3_industrial',
            'SERVICIOS': '4_1_office + 4_2_retail + 4_3_publicServices',
            'NO ESPECIFICADO': '0_unknown'
        })
        df_melted = df_melted.drop(columns=['index', 'dataYear', 'dataMonth', 'dataDay', 'dataHour', 'community', 'province',
                              'municipality', 'distributor', 'measurePointType', 'tension', 'fare',
                              'timeDiscrimination', 'sumEnergy'])

        df_melted.sort_values('date', inplace=True)


        # # Aggregate results by postalCode and day
        # df_melted = df_melted.groupby(['postalCode', 'economicSector', 'tariff', 'date']).sum()
        # df_melted = df_melted.reset_index()











    # df_cat_08011 = pd.read_csv("/Users/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/08011catastro.csv")
    # cat_ref_list = df_cat_08011.reference.to_list()
    #
    # df_consumptions = pd.read_excel('data/AMB_consumptions/Dades vulnerabilitat energètica habitatges_CIMNE.xlsx', engine='openpyxl')
    # df_building_consumption = df_consumptions[df_consumptions.REFCAT.isin(cat_ref_list)]
    #
    # df_building_consumption.to_json('data/AMB_consumptions/data.json', orient='records')
    # df_dict = {}
    # df_dict['consumptions'] = json.loads(df_building_consumption.to_json(orient="records"))
    # with open("data/AMB_consumptions/data.json", "w") as d_file_data:
    #     json.dump(df_dict, d_file_data)
    morph_config = '\n[DataSource1]\nmappings:data/AMB_consumptions/mapping.yaml\nfile_path: {d_file}\n'
    json_data = json.load(open("data/AMB_consumptions/data.json"))
    df_data_consumptions = pd.json_normalize(json_data['consumptions'])

    freq = "P1Y"
    user = "CRBCN"

    header = ["REFCAT", "date", "start", "bucket", "end", "isReal", "electricityValue"]
    df_data_consumptions.to_csv('data/AMB_consumptions/AMB08011.csv', columns=header)

    df_data_consumptions["electricityDeviceId"] = df_data_consumptions['REFCAT'].apply(lambda x: (x + '-Electricity').encode("utf-8"))
    df_data_consumptions["electricityDeviceId"] = df_data_consumptions['electricityDeviceId'].apply(
        lambda x: hashlib.sha256(x).hexdigest())

    with open("sources/AMB_consumptions/harmonizer/temp.json", "w") as amb_file:
        json.dump({"consumptions": df_to_formatted_json(df_data_consumptions, sep=".")}, amb_file)
    #######--------------------
    # for root, dirs, files in os.walk(wd_datadis):
    #     for fname in files:
    #         if re.match("^.*.csv$", fname):
    #             df = pd.read_csv(os.path.join(root, fname), sep="\t",
    #                              index_col=0, dtype=str)
    #             df_all.append(df)
    #             del df
    #
    # df_all = pd.concat(df_all)
    #
    # unique_postal_codes = list(df_all.postalCode.unique())
    # df_g = df_all.groupby("postalCode")
    # del df_all
    #
    # print('\nMerging DATADIS and meteo datasets to the geographical layers, and uploading all to MongoDB.')
    #
    # for postal_code in unique_postal_codes:
    #     print(postal_code)
    #
    #     # Select the postal code group
    #     df = copy.deepcopy(df_g.get_group(postal_code))
    #     df.postalCode = postal_code
    #     # Cast numeric columns to float
    #     for col in ['sumEnergy', 'sumContracts', 'mi1', 'mi2', 'mi3', 'mi4', 'mi5', 'mi6', 'mi7', 'mi8',
    #                 'mi9', 'mi10', 'mi11', 'mi12', 'mi13', 'mi14', 'mi15', 'mi16', 'mi17', 'mi18', 'mi19',
    #                 'mi20', 'mi21', 'mi22', 'mi23', 'mi24']:
    #         df[col] = df[col].astype(float)
    #
    #     # Delete when mi25>0
    #     df = df[df.mi25 == "0"]
    #
    #     # Add the date column
    #     df["date"] = pd.to_datetime(dict(year=df.dataYear, month=df.dataMonth, day=df.dataDay))
    #
    #     # Delete duplicates and drop unnecessary columns
    #     df = df[~df.duplicated(
    #         subset=['municipality', 'distributor', 'measurePointType', 'postalCode', 'economicSector', 'fare',
    #                 'timeDiscrimination', 'date'], keep='last')]
    #     df = df.reset_index()
    #
    #     # fare, timeDiscrimination and economicSector to tariff
    #     df["tariff"] = df["economicSector"] + "~" + df["timeDiscrimination"] + "~" + df["fare"]
    #     df["tariff"] = df["tariff"].map({
    #         "INDUSTRIA~GENERAL ALTA TENSIÓN~>= 1 kV Y < 30 kV": "3.1A",
    #         "INDUSTRIA~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
    #         "INDUSTRIA~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
    #         "INDUSTRIA~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
    #         "INDUSTRIA~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
    #         "INDUSTRIA~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
    #         "NO ESPECIFICADO~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
    #         "NO ESPECIFICADO~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
    #         "NO ESPECIFICADO~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
    #         "NO ESPECIFICADO~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHS",
    #         "NO ESPECIFICADO~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
    #         "NO ESPECIFICADO~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
    #         "RESIDENCIAL~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
    #         "RESIDENCIAL~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
    #         "RESIDENCIAL~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
    #         "RESIDENCIAL~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHS",
    #         "RESIDENCIAL~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
    #         "RESIDENCIAL~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A",
    #         "SERVICIOS~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0DHA",
    #         "SERVICIOS~TARIFA DE DOS PERIODOS~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1DHA",
    #         "SERVICIOS~TARIFA DE TRES PERIODOS~GENERAL BAJA TENSIÓN": "3.0A",
    #         "SERVICIOS~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA NO SUPERIOR A 10 kW": "2.0A",
    #         "SERVICIOS~TARIFA DE UN PERIODO~GENERAL BAJA TENSIÓN, POTENCIA SUPERIOR A 10 kW Y NO SUPERIOR A 15 kW": "2.1A"
    #     })
    #     df["economicSector"] = df["economicSector"].map({
    #         'RESIDENCIAL': '1_residential',
    #         'INDUSTRIA': '2_agriculture + 3_industrial',
    #         'SERVICIOS': '4_1_office + 4_2_retail + 4_3_publicServices',
    #         'NO ESPECIFICADO': '0_unknown'
    #     })
    #     df = df.drop(columns=['index', 'dataYear', 'dataMonth', 'dataDay', 'community', 'province',
    #                           'municipality', 'distributor', 'measurePointType', 'tension', 'fare',
    #                           'timeDiscrimination', 'sumEnergy'])
    #
    #     # Aggregate results by postalCode and day
    #     df = df.groupby(['postalCode', 'economicSector', 'tariff', 'date']).sum()
    #     df = df.reset_index()
    #     if not df.empty:
    #         # Reshape wide to long
    #         df_l = pd.melt(df,
    #                        value_vars=['mi%s' % i for i in range(1, 25)],
    #                        id_vars=['postalCode', 'economicSector', 'tariff',
    #                                 'date', 'sumContracts'],
    #                        var_name="hour", value_name="energy")
    #         # Rearrange time, hour and energy columns
    #         df_l.hour = np.core.defchararray.replace(np.array(df_l.hour).astype(str), "mi", "").astype(int) - 1
    #         df_l.energy = np.array(df_l.energy).astype(float)
    #         df_l["time"] = np.core.defchararray.add(
    #             np.core.defchararray.replace(np.array(df_l.date.dt.date).astype(str), "-", ""),
    #             np.core.defchararray.zfill(np.array(df_l.hour).astype(str), 2)
    #         )
    #         df_l.time = pd.to_datetime(df_l.time, format="%Y%m%d%H").dt.tz_localize(tz="Europe/Madrid", ambiguous='NaT',
    #                                                                                 nonexistent='NaT')
    #         df_l = df_l[~pd.isnull(df_l.time)]
    #         df_l.time = df_l.time.dt.tz_convert("UTC")
    #         load_datadis_to_mongo(db, df_l, configuration_data)
    #     else:
    #         print('-------------------------------------------------------------------------')
    #     '''print(np.min(df.date))
    #     print(np.max(df.date))'''
    #     get_meteo_and_load_to_mongo(
    #         db=db,
    #         postal_codes=postal_code,
    #         centroids_postal_code_file='%s/cpcentroid.csv' % wd_postal_codes,
    #         wd=wd_meteo,
    #         configuration_data=configuration_data,
    #         start_date=dt.datetime.strptime(configuration_data.get('datadis').get('startDate'), "%Y/%m/%d").date(),
    #         end_date=dt.datetime.strptime(configuration_data.get('datadis').get('endDate'), "%Y/%m/%d").date())
    #     '''startDate=np.min(df.date),
    #         endDate=np.max(df.date))'''
    #     time.sleep(0.1)
    #############################


    # g_rdflib = morph_kgc.materialize(morph_config.format(d_file=amb_file.name))
    # os.unlink("sources/AMB_consumptions/harmonizer/temp.json")
    # neo = GraphDatabase.driver(**config['neo4j'])
    # content = g_rdflib.serialize(format="ttl")
    # content = content.replace('\\"', "&apos;")
    # content = content.replace("'", "&apos;")
    # with neo.session() as s:
    #     response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
    #     print(response.single())
    #
    # hbase_conn = config['hbase_store_harmonized_data']
    # df_data_consumptions['date'] = pd.to_datetime("2017/1/1")
    # df_data_consumptions['start'] = df_data_consumptions['date'].astype(int) // 10 ** 9
    # df_data_consumptions["bucket"] = (df_data_consumptions['start'].apply(
    #     int) // settings.ts_buckets) % settings.buckets
    # df_data_consumptions['end'] = (df_data_consumptions['date'] + timedelta(days=365)).astype(int) // 10 ** 9
    # df_data_consumptions['isReal'] = True
    # df_data_consumptions['electricityValue'] = df_data_consumptions['Total electricitat (kWh/m²)'] * \
    #                                            df_data_consumptions['V306_SCT.Scons.total']
    #
    # electricity_device_table = f"harmonized_online_EnergyConsumptionGridElectricity_100_SUM_{freq}_{user}"
    # save_to_hbase(df_data_consumptions.to_dict(orient="records"), electricity_device_table, hbase_conn,
    #               [("info", ['end', 'isReal']), ("v", ['electricityValue'])],
    #               row_fields=['bucket', 'electricityDeviceId', 'start'])
    # electricity_period_table = f"harmonized_batch_EnergyConsumptionGridElectricity_100_SUM_{freq}_{user}"
    # save_to_hbase(df_data_consumptions.to_dict(orient="records"), electricity_period_table, hbase_conn,
    #               [("info", ['end', 'isReal']), ("v", ['electricityValue'])],
    #               row_fields=['bucket', 'start', 'electricityDeviceId'])


