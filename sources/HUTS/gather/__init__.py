import argparse
import os
import pandas as pd
import utils

def gather_data(config, settings, args):
    for file in os.listdir('data/HUTS'):
        if file.endswith('.xlsx'):
            df = pd.read_excel(f"data/HUTS/{file}",
                               skiprows=2)  # todo: change way to get input

            save_HUTS_data(
                data=pd.DataFrame(data={'CUPS': df['CUPS'].unique(), 'devices': df['CUPS'].unique()}).to_dict(
                    orient="records"),
                data_type="devices",
                row_keys=["devices"],
                column_map=[("info", "all")], config=config, settings=settings, args=args)

            save_HUTS_data(data=df.to_dict(orient='records'), data_type="invoices",
                             row_keys=["CUPS", "Fecha inicio Docu. cálculo"],
                             column_map=[("info", "all")], config=config, settings=settings, args=args)

def save_HUTS_data(data, data_type, row_keys, column_map, config, settings, args):
    if args.store == "kafka":
        try:
            k_topic = config["kafka"]["topic"]
            kafka_message = {
                "namespace": args.namespace,
                "user": args.user,
                "timezone": args.timezone,
                "collection_type": data_type,
                "source": config['source'],
                "row_keys": row_keys,
                "data": data
            }
            utils.kafka.save_to_kafka(topic=k_topic, info_document=kafka_message,
                                      config=config['kafka']['connection'], batch=settings.kafka_message_size)

        except Exception as e:
            utils.utils.log_string(f"error when sending message: {e}")

    elif args.store == "hbase":

        try:
            h_table_name = f"{config['data_sources'][config['source']]['hbase_table']}_ts_{data_type}_invoices_{args.user}"  # todo : change

            utils.hbase.save_to_hbase(data, h_table_name, config['hbase_store_raw_data'], column_map,
                                      row_fields=row_keys)
        except Exception as e:
            utils.utils.log_string(f"Error saving datadis supplies to HBASE: {e}")
    else:
        utils.utils.log_string(f"store {config['store']} is not supported")


def gather(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Gathering data from Inspire')
    ap.add_argument("-st", "--store", required=True, help="Where to store the data", choices=["kafka", "hbase"])
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--timezone", "-tz", help="The local timezone", required=True, default='Europe/Madrid')
    args = ap.parse_args(arguments)

    gather_data(config=config, settings=settings, args=args)
