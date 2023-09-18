import os
from dotenv import load_dotenv

from ontology.namespaces_definition import bigg_enums

load_dotenv()
conf_file = os.getenv("CONFIG_FILE")
kafka_message_size = 10
secret_password = os.getenv("SECRET_PASSWORD")
namespace_mappings = {"bigg": "bigg", "wgs": "wgs"}
ts_buckets = 10000000
buckets = 20

sources_priorities = ["Org", "GPG", "Bulgaria", "BulgariaEPC", "BIS", "gemweb", "datadis", "CEEC3X",
                      "genercat", "nedgia", "weather", "analytics", "OpenData"]

countries = ["ES", "BG"]

