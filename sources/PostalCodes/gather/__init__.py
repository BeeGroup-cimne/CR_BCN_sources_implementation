import argparse
import os

import pandas as pd

import utils


def gather_data(config, settings, args):
    for file in os.listdir('data/PostalCodes'):
        if file.endswith('.xlsx'):
            df = pd.read_excel(f"data/PostalCodes/{file}",
                               skiprows=2)  # todo: change way to get input

def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Gathering data from Inspire')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--timezone", "-tz", help="The local timezone", required=True, default='Europe/Madrid')
    args = ap.parse_args(arguments)

    gather_data(config=config, settings=settings, args=args)
