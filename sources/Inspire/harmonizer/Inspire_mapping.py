import json
import os
import tempfile
from functools import partial
import utils
from neo4j import GraphDatabase
from slugify import slugify

import settings
import morph_kgc
import pandas as pd
import rdflib
from utils.data_transformations import fuzzy_dictionary_match, fuzz_params

morph_config = """
[DataSource1]
mappings: data/Inspire/building/mapping.yaml
file_path: {d_file}
"""

config = utils.utils.read_config(settings.conf_file)

"""    
import json
import tempfile
import os
import morph_kgc
import pandas as pd
import rdflib
morph_config = '\n[DataSource1]\nmappings:data/Inspire/building/mapping.yaml\nfile_path: {d_file}\n'
json_data = json.load(open("data/Inspire/building/data.json"))

######buildingspace
morph_config = '\n[DataSource1]\nmappings:data/Inspire/buildingSpace/mapping.yaml\nfile_path: {d_file}\n'
json_data = json.load(open("data/Inspire/buildingSpace/data.json"))


##df_buildings = pd.DataFrame.from_records(json_data['buildings'])
# df = pd.DataFrame.from_dict(json_data['buildings']['punto'])
# df_mun = pd.DataFrame.from_records(json_data['Localitats'])
# df_buildings['MUNICIPI_NAME'] = df_buildings['MUNICIPI_ID'].map(df_mun[['MUNICIPI_ID', 'MUNICIPI_DESC']].set_index("MUNICIPI_ID").MUNICIPI_DESC)

"""
# def clean_municipality(df_buildings):
#     g = rdflib.Graph()
#     g.parse("ontology/all-geonames-rdf-clean-ES.rdf")
#     mun_fuzz = partial(fuzzy_dictionary_match,
#                             map_dict=fuzz_params(g, ['gn:name']),
#                             default=None)
#     mun_unique = df_buildings['MUNICIPI_NAME'].unique()
#     mun_map = {x: mun_fuzz(x) for x in mun_unique}
#     df_buildings['MUNICIPI_URI'] = df_buildings['MUNICIPI_NAME'].map(mun_map)
#     return df_buildings


# def clean_address(df_buildings):
#     df_buildings['ADREÇA_URI'] = df_buildings['CP'] + "-" + df_buildings['ADREÇA'].apply(lambda x: slugify(x))
#     return df_buildings
#

# def clean_lat_lon(df_buildings):
#     df_buildings['COORDENADES_LAT'] = df_buildings['COORDENADES'].apply(lambda x: x.split(",")[1])
#     df_buildings['COORDENADES_LON'] = df_buildings['COORDENADES'].apply(lambda x: x.split(",")[0])
#     return df_buildings


def building_taxonomies(df_point):

    # conditionOfConstruction
    condiction_of_construction_dict = pd.read_excel('sources/Inspire/harmonizer/BuildingInspireTaxonomy.xlsx', sheet_name='conditionOfConstruction', engine='openpyxl').set_index('SOURCE')['TAXONOMY'].to_dict()
    df_point['properties.conditionOfConstruction'] = df_point['properties.conditionOfConstruction'].apply(lambda x: condiction_of_construction_dict[x])

    # mainUse
    main_use_dict = pd.read_excel('sources/Inspire/harmonizer/BuildingInspireTaxonomy.xlsx', sheet_name='mainUse',
                  engine='openpyxl').set_index('SOURCE')['TAXONOMY'].to_dict()
    df_point['properties.currentUse'] = df_point['properties.currentUse'].apply(
        lambda x : main_use_dict[x] if x in main_use_dict.keys() else None)

    # officialAreaReference
    official_area_reference_dict = pd.read_excel('sources/Inspire/harmonizer/BuildingInspireTaxonomy.xlsx', sheet_name='officialAreaReference',
                  engine='openpyxl').set_index('SOURCE')['TAXONOMY'].to_dict()
    df_point['properties.officialAreaReference'] = df_point['properties.officialAreaReference'].apply(
        lambda x: official_area_reference_dict[x])
    return df_point

def address_taxonomy(df_point):
    specification_dict = pd.read_excel('sources/Inspire/harmonizer/AddressInspireTaxonomy.xlsx', sheet_name='specification', engine='openpyxl').set_index('SOURCE')['TAXONOMY'].to_dict()
    df_point['properties.specification'] = df_point['properties.specification'].apply(lambda x: specification_dict[x])
    return df_point

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

def get_address_street_name(df_point):

    address_street_names_df = pd.read_csv("data/Inspire/address/addressTh08900.csv", usecols = ['gml_id','text'])
    address_street_names_df["gml_id"] = address_street_names_df['gml_id'].apply(lambda x: x.split('ES.SDGC.TN.')[1])
    df_point["properties.merge_gml_id"] = df_point['properties.gml_id'].apply(lambda x: '.'.join(x.split('ES.SDGC.AD.')[1].split('.')[:3]))
    merge_df = pd.merge(df_point, address_street_names_df, left_on="properties.merge_gml_id", right_on="gml_id")
    merge_df.drop('gml_id', inplace=True, axis=1)
    merge_df.rename(columns={'text': 'properties.street_name'}, inplace=True)
    return merge_df

def coordinate_to_string(json_data):
    # json_data = centroid_data
    for item in json_data['features']:
        print(item['geometry']['coordinates'])
def harmonize_buildings_static(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    df_buildings = pd.DataFrame(data)

    # centroid_data = json.load(open("data/Inspire/building/08011buildingsCentroid.geojson"))
    # geojson_data = json.load(open("data/Inspire/building/08011buildingsGeoJSON.geojson"))
    #
    # with open("data/Inspire/building/data.json", "w") as d_file:
    #     json.dump({"buildings": {"point": centroid_data['features'], "geojson": geojson_data['features']}}, d_file)

    morph_config = '\n[DataSource1]\nmappings:data/Inspire/building/mapping.yaml\nfile_path: {d_file}\n'
    json_data = json.load(open("data/Inspire/building/data.json"))


    df_point = pd.json_normalize(json_data['buildings']['point'])
    df_point['properties.building_gml_id'] = df_point['properties.gml_id'].apply(lambda x: x.split('_')[0].split('BU.')[1])
    df_point['geometry.coordinates'] = df_point['geometry.coordinates'].apply(lambda x: ' '.join([str(item) for item in x]))
    df_point = building_taxonomies(df_point)

    df_geojson = pd.json_normalize(json_data['buildings']['geojson'])
    df_geojson['properties.building_gml_id'] = df_geojson['properties.gml_id'].apply(
        lambda x: x.split('_')[0].split('BU.')[1])
    df_geojson['geometry.coordinates'] = df_geojson['geometry.coordinates'].apply(lambda x: ' '.join([str(item) for item in x]))


    with open("sources/Inspire/harmonizer/temp.json", "w") as d_file:
        json.dump({"buildings": {"point": df_to_formatted_json(df_point, sep="."), "geojson": df_to_formatted_json(df_geojson, sep=".")}}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/Inspire/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())
def harmonize_buildings_space_static(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)

    centroid_data = json.load(open("data/Inspire/buildingSpace/08011buildingSpaceCentroid.geojson"))
    geojson_data = json.load(open("data/Inspire/buildingSpace/08011buildingSpaceGeoJSON.geojson"))

    with open("data/Inspire/buildingSpace/data.json", "w") as d_file:
        json.dump({"buildingsSpaces": {"point": centroid_data['features'], "geojson": geojson_data['features']}}, d_file)

    morph_config = '\n[DataSource1]\nmappings:data/Inspire/buildingSpace/mapping.yaml\nfile_path: {d_file}\n'
    json_data = json.load(open("data/Inspire/buildingSpace/data.json"))

    df_point = pd.json_normalize(json_data['buildingsSpaces']['point'])
    #building gml_id
    df_point['properties.building_gml_id'] = df_point['properties.gml_id'].apply(lambda x: x.split('_')[0].split('BU.')[1])
    df_point['geometry.coordinates'] = df_point['geometry.coordinates'].apply(lambda x: ' '.join([str(item) for item in x]))

    df_geojson = pd.json_normalize(json_data['buildingsSpaces']['geojson'])
    df_geojson['geometry.coordinates'] = df_geojson['geometry.coordinates'].apply(lambda x: ' '.join([str(item) for item in x]))

    with open("sources/Inspire/harmonizer/temp.json", "w") as d_file:
        json.dump({"buildingsSpaces": {"point": df_to_formatted_json(df_point, sep="."), "geojson": df_to_formatted_json(df_geojson, sep=".")}}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/Inspire/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())

def harmonize_address_static(data, **kwargs):
    # config = utils.utils.read_config(settings.conf_file)
    # df_buildings = pd.DataFrame(data)

    import json
    import tempfile
    import os
    import morph_kgc
    import pandas as pd
    import rdflib
    morph_config = '\n[DataSource1]\nmappings:data/Inspire/address/mapping.yaml\nfile_path: {d_file}\n'

    centroid_data = json.load(open("data/Inspire/address/address08011.geojson"))

    with open("data/Inspire/address/data.json", "w") as d_file:
        json.dump({"address": {"point": centroid_data['features']}}, d_file)

    json_data = json.load(open("data/Inspire/address/data.json"))
    df_point = pd.json_normalize(json_data['address']['point'])

    #building gml_id
    df_point['properties.building_gml_id'] = df_point['properties.gml_id'].apply(lambda x: x.split('.')[-1])
    df_point['geometry.coordinates'] = df_point['geometry.coordinates'].apply(
        lambda x: ' '.join([str(item) for item in x]))
    df_point = get_address_street_name(df_point)
    df_point = address_taxonomy(df_point)

    with open("sources/Inspire/harmonizer/temp.json", "w") as d_file:
        json.dump({"address": {"point": df_to_formatted_json(df_point, sep=".")}}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/Inspire/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())