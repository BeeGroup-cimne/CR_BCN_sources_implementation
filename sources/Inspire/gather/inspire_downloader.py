import os
from zipfile import ZipFile
import requests
import yaml
from bs4 import BeautifulSoup
from pymongo import MongoClient

address_url = "https://www.catastro.minhap.es/INSPIRE/addresses/08/ES.SDGC.ad.atom_08.xml"
building_url = "https://www.catastro.minhap.es/INSPIRE/buildings/08/ES.SDGC.bu.atom_08.xml"
cr_bcn_postal_codes = ['08033', '08034', '08002', '08005', '08004', '08003', '08035', '08032', '08010', '08017', '08028', '08021', '08026', '08019', '08027', '08018', '08020', '08016', '08029', '08011', '08042', '08008', '08037', '08030', '08039', '08006', '08001', '08038', '08007', '08031', '08009', '08036', '08040', '08014', '08013', '08025', '08022', '08041', '08023', '08024', '08012', '08015']


def download(url, name, save_path):
    get_response = requests.get(url, stream=True)
    if get_response:
        file_name = os.path.join(save_path, name)
        with open(file_name, 'wb') as f:
            for chunk in get_response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
    else:
        print(get_response)

def find_download(url, save_path):
    link_list = []
    downloaded_link_list = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("link")
    for link in links:
        link_url = link["href"]
        if len(link_url.split('.')) == 9 and link_url.split('.')[7] in cr_bcn_postal_codes and link_url.split('.')[8] == 'zip':
            download(link_url, link_url.split('.')[7], save_path + '/zip/')

def unzip_directory(zip_directory, unzip_directory):
    for file in os.listdir(zip_directory):
        with ZipFile(zip_directory + '/' + file, 'r') as zip:
            zip.extractall(unzip_directory)

def get_data():
    # find_download(address_url, "/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address")
    # unzip_directory("/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/zip", "/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/unzip")
    find_download(address_url, "/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building")
    unzip_directory("/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/zip", "/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/unzip")


    # get_address_coord_data('data/Inspire/download/address')
    # get_building_data('data/Inspire/download/building')

if __name__ == '__main__':

    # with open("../C3/configuration.yaml", "r") as stream:
    #     configuration_data = yaml.safe_load(stream)

    # catastro_data_08 = str(configuration_data.get('paths').get('tempFolderPath')) + 'catastroData/08'
    #
    # if not os.path.exists(str(configuration_data.get('paths').get('tempFolderPath')) + 'catastroData'):
    #     os.mkdir(str(configuration_data.get('paths').get('tempFolderPath')) + 'catastroData')
    # if not os.path.exists(catastro_data_08):
    #     os.mkdir(catastro_data_08)
    # if not os.path.exists(catastro_data_08 + '/unzip'):
    #     os.mkdir(catastro_data_08 + '/unzip')
    # if not os.path.exists(catastro_data_08 + '/zip'):
    #     os.mkdir(catastro_data_08 + '/zip')
    get_data()

