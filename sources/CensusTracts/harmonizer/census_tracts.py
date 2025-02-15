import json
import os
import utils
from neo4j import GraphDatabase
from helpers import df_to_formatted_json

import settings
import morph_kgc
import pandas as pd
import rdflib

def harmonize_census_tracts(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_geometry = pd.DataFrame(data)
    morph_config = '\n[DataSource1]\nmappings:data/CensusTracts/mapping.yaml\nfile_path: {d_file}\n'

    data = json.load(open("data/CensusTracts/bcn_census_tracts.geojson"))

    with open("data/CensusTracts/data.json", "w") as d_file:
        json.dump({"censusTracts": {"geojson": data['features']}}, d_file)

    json_data = json.load(open("data/CensusTracts/data.json"))
    df_geometry = pd.json_normalize(json_data['censusTracts']['geojson'])

    df_geometry['geometry.coordinates'] = df_geometry['geometry.coordinates'].apply(
        lambda x: ' '.join([str(item) for item in x]))


    with open("sources/CensusTracts/harmonizer/temp.json", "w") as d_file:
        json.dump({"censusTracts": {"geojson": df_to_formatted_json(df_geometry, sep=".")}}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/CensusTracts/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
        print(response.single())
