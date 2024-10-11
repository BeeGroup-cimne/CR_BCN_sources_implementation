import json
import os
import tempfile
from functools import partial
import utils
from neo4j import GraphDatabase

import settings
import morph_kgc
import pandas as pd
import rdflib
from utils.data_transformations import fuzzy_dictionary_match, fuzz_params
from helpers import df_to_formatted_json


def harmonize_neighbourhoods(data, **kwargs):
    config = utils.utils.read_config(settings.conf_file)
    # df_geometry = pd.DataFrame(data)
    morph_config = '\n[DataSource1]\nmappings:data/Neighbourhoods/mapping.yaml\nfile_path: {d_file}\n'

    data = json.load(open("data/Neighbourhoods/bcn_neighbourhoods.geojson"))

    with open("data/Neighbourhoods/data.json", "w") as d_file:
        json.dump({"neighbourhoods": {"geojson": data['features']}}, d_file)

    json_data = json.load(open("data/Neighbourhoods/data.json"))
    df_geometry = pd.json_normalize(json_data['neighbourhoods']['geojson'])

    df_geometry['geometry.coordinates'] = df_geometry['geometry.coordinates'].apply(
        lambda x: ' '.join([str(item) for item in x]))
    df_geometry['properties.codi_districte'] = df_geometry['properties.codi_districte'].apply(
        lambda x: str(x).zfill(2))

    with open("sources/Neighbourhoods/harmonizer/temp.json", "w") as d_file:
        json.dump({"neighbourhoods": {"geojson": df_to_formatted_json(df_geometry, sep=".")}}, d_file)

    g_rdflib = morph_kgc.materialize(morph_config.format(d_file=d_file.name))
    os.unlink("sources/Neighbourhoods/harmonizer/temp.json")
    neo = GraphDatabase.driver(**config['neo4j'])
    content = g_rdflib.serialize(format="ttl")
    content = content.replace('\\"', "&apos;")
    content = content.replace("'", "&apos;")
    with neo.session() as s:
        response = s.run(f"""CALL n10s.rdf.import.inline('{content}','Turtle')""")
    print(response.single())
