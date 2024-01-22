import copy
import csv
import datetime as dt
import forecastio
import multiprocessing.pool as mp
import os
import pytz
import re
import sys
import time
import urllib
import yaml
import numpy as np
import pandas as pd
from beedis.datadis import datadis, ENDPOINTS
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient


def get_postal_codes(db):
    postal_codes = []
    os.makedirs(configuration_data.get('storePaths').get('postalCodes'), exist_ok=True)
    with open(configuration_data.get('storePaths').get('postalCodes') + '/cpcentroid.csv', 'w') as csvfile:
        field_names = ['CODPOS', 'ycoord', 'xcoord']
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        doc = db.postalCodes.find()  # ['entityId']
        for x in doc:
            if x['description']['centroid'][0] != 'NULL' or x['description']['centroid'][1] != 'NULL':
                writer.writerow({'CODPOS': str(x['entityId']),
                                 'ycoord': x['description']['centroid'][0],
                                 'xcoord': x['description']['centroid'][1]})
                postal_codes.append(x['entityId'])
    return postal_codes

def datadis_query(db, configuration_data):

    # crbcn postalCodes ['08033', '08034', '08002', '08005', '08004', '08003', '08035', '08032', '08010', '08017', '08028', '08021', '08026', '08019', '08027', '08018', '08020', '08016', '08029', '08011', '08042', '08008', '08037', '08030', '08039', '08006', '08001', '08038', '08007', '08031', '08009', '08036', '08040', '08014', '08013', '08025', '08022', '08041', '08023', '08024', '08012', '08015']
    postal_codes = ['08033', '08034', '08002', '08005', '08004', '08003', '08035', '08032', '08010', '08017', '08028', '08021', '08026', '08019', '08027', '08018', '08020', '08016', '08029', '08011', '08042', '08008', '08037', '08030', '08039', '08006', '08001', '08038', '08007', '08031', '08009', '08036', '08040', '08014', '08013', '08025', '08022', '08041', '08023', '08024', '08012', '08015']
    ############################
    datadis.connection(configuration_data.get('datadis').get('user'), configuration_data.get('datadis').get('password'),
                       timezone="UTC")
    time_discrimination = configuration_data.get('datadis').get('timeDiscrimination')
    fare = configuration_data.get('datadis').get('fare')
    economic_sector = configuration_data.get('datadis').get('economicSector')
    tension = configuration_data.get('datadis').get('tension')

    for postal_code in postal_codes:
        start_date = dt.datetime.strptime(configuration_data.get('crbcnDatadis').get('startDate'), "%Y/%m/%d").date()
        end_date = dt.datetime.strptime(configuration_data.get('crbcnDatadis').get('endDate'), "%Y/%m/%d").date()
        os.makedirs("%s/%s" % (configuration_data.get('storePaths').get('crbcnDatadis'), postal_code), exist_ok=True)
        print(f'\nDownloading DATADIS consumption data from CP {postal_code}.')
        while start_date < end_date:
            datadis_dataset = []

            if not str(start_date)[0:7] in os.listdir(
                    "%s/%s" % (configuration_data.get('storePaths').get('crbcnDatadis'), postal_code)) or \
                    not "consumption.csv" in os.listdir("%s/%s/%s" % (
                            configuration_data.get('storePaths').get('crbcnDatadis'), postal_code, str(start_date)[0:7])):
                os.makedirs(
                    "%s/%s/%s" % (configuration_data.get('storePaths').get('crbcnDatadis'), postal_code, str(start_date)[0:7]),
                    exist_ok=True)

                datadis_dataset = datadis.datadis_query(ENDPOINTS.GET_PUBLIC,
                                                       start_date=start_date,
                                                       end_date=(start_date + relativedelta(months=1) - relativedelta(
                                                           days=1)),
                                                       page=0,
                                                       fare=fare,
                                                       economic_sector=economic_sector,
                                                       tension=tension,
                                                       time_discrimination=time_discrimination,
                                                       community="09",
                                                       page_size=2000,
                                                       postal_code=postal_code)
                pd.DataFrame(datadis_dataset).to_csv("%s/%s/%s/consumption.csv" % (
                    configuration_data.get('storePaths').get('crbcnDatadis'), postal_code, str(start_date)[0:7]),
                                                    sep="\t")

            start_date += relativedelta(months=+1)
            time.sleep(0.1)

if __name__ == '__main__':
    with open("../C3/configuration.yaml", "r") as stream:
        configuration_data = yaml.safe_load(stream)

    datadis_query(db, configuration_data)


