import utils.utils
from neo4j import GraphDatabase
import settings
import rdflib
config = utils.utils.read_config(settings.conf_file)
neo = GraphDatabase.driver(**config['neo4j'])
with open('/Users/jose/PycharmProjects/CR_BCN_sources_implementation/data/GeoNames/all-geonames-rdf-clean-ES.txt') as f:
    content = f.read()
content = content.replace('\\"', "&apos;")
content = content.replace("'", "&apos;")

with neo.session() as s:
    # a = f"""CALL n10s.rdf.import.inline('{content}','RDF/XML')"""
    response = s.run(f"""CALL n10s.rdf.import.inline('{content}','RDF/XML')""")
    # response = s.run(f"""Match(n) return n limit 2""")
    print(response.single())
