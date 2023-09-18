from ontology.namespaces_definition import bigg_enums


def postal_code_correction_function(b, stations):
    tries = [1, -1]
    if int(b['station_code'][-1]) == 0:
        tries = [1]
    if int(b['station_code'][-1]) == 9:
        tries = [-1]
    for t in tries:
        try:
            test = str(int(b['station_code']) + t).zfill(5)
            return stations['latitude', 'longitude'].loc[test].items()
        except:
            pass

ZONE_DICTIONARY = {
    "ES": {
        "file": "/Users/eloigabal/Downloads/ES/all-geonames-rdf-clean-ES.txt",
        "adms": [
            ("province", "A.ADM2", "AddressProvince"),
            ("municipality", "A.ADM3", "AddressCity"),
        ]
    },
    "BG": {
        "file": "/Users/eloigabal/Downloads/ES/all-geonames-rdf-clean-BG.txt",
        "adms":[
            ("province", "A.ADM1", "AddressProvince"),
            ("municipality", "A.ADM2", "AddressCity")
        ]
    }
}

WEATHER_STATIONS = {
    "ES": {
        "weather_query":
            """
                Match (bs:{bigg}__BuildingSpace)<-[]-(n:{bigg}__Building)-[:{bigg}__hasLocationInfo]->(l:{bigg}__LocationInfo) 
                WHERE l.{bigg}__addressPostalCode IS NOT NULL and split(l.uri,"#")[0] in {namespaces}
                RETURN bs.uri as subject, l.{bigg}__addressPostalCode as station_code
            """,
        "namespaces": ["https://icaen.cat"],
        "weather_correction_function": postal_code_correction_function
    },
    "BG": {
        "weather_query":
            """
                Match (bs:{bigg}__BuildingSpace)<-[]-(n:{bigg}__Building)-[:{bigg}__hasLocationInfo]->(l:{bigg}__LocationInfo) 
                WHERE split(l.uri,"#")[0] in {namespaces}
                RETURN bs.uri as subject, [(l)-[:{bigg}__hasAddressCity{{selected:true}}]->(ad) | ad.geo__name][0] as station_code
            """,
        "namespaces": ["https://bulgaria.bg"]
    }
}

electricity_agg_cat = [
    {
        "measured_property": bigg_enums.EnergyConsumptionGridElectricity,
        "device_query": f"""'bigg__Device' in labels(d) AND d.source='DatadisSource'""",
        "freq": "PT1H",
        "agg_name": "totalElectricityConsumption",
        "required": "true",
        "agg_func": ["SUM"]
    }
]
gas_agg_cat = [
    {
        "measured_property": bigg_enums.EnergyConsumptionGas,
        "device_query": f"""'bigg__Device' in labels(d) AND d.source='NedgiaSource'""",
        "freq": "",
        "agg_name": "totalGasConsumption",
        "required": "true",
        "agg_func": ["SUM"]
    }
]

electricity_agg_bg = [
    {
        "measured_property": bigg_enums.EnergyConsumptionGridElectricity,
        "device_query": f"""'bigg__Device' in labels(d) AND d.source='SummarySource'""",
        "freq": "P1Y",
        "agg_name": "totalElectricityConsumption",
        "required": "true",
        "agg_func": ["SUM"]
    }
]

gas_agg_bg = [
    {
        "measured_property": bigg_enums.EnergyConsumptionGas,
        "device_query": f"""'bigg__Device' in labels(d) AND d.source='SummarySource'""",
        "freq": "P1Y",
        "agg_name": "totalGasConsumption",
        "required": "true",
        "agg_func": ["SUM"]
    }
]

outdoor_weather_device_agg = [
    {
        "measured_property": bigg_enums.Temperature,
        "device_query": f"""'bigg__WeatherStation' in labels(d)""",
        "freq": "PT1H",
        "agg_name": "outdoorTemperature",
        "required": "true",
        "agg_func": ["AVG", "CDD", "HDD"]
    },
    {
        "measured_property": bigg_enums.HumidityRatio,
        "device_query": f"""'bigg__WeatherStation' in labels(d)""",
        "freq": "PT1H",
        "agg_name": "outdoorHumidityRatio",
        "required": "false",
        "agg_func": ["AVG"]
    }
]

DEVICE_AGGREGATORS = {
    "ES": {
        "totalGasConsumption": gas_agg_cat,
        "totalElectricityConsumption": electricity_agg_cat,
        "externalWeather": outdoor_weather_device_agg
    },
    "BG": {
        "totalGasConsumption": gas_agg_bg,
        "totalElectricityConsumption": electricity_agg_bg,
        "externalWeather": outdoor_weather_device_agg
        }
}


NON_USED_COUNTRIES_IN_BIGG = {
    "GR": {
        "file": "/Users/eloigabal/Downloads/ES/all-geonames-rdf-clean-GR.txt",
        "adms": [
            ("province", "A.ADM1", "AddressProvince"),
            ("municipality", "A.ADM3", "AddressCity")
        ]
    },
    "CZ": {
        "file": "/Users/eloigabal/Downloads/ES/all-geonames-rdf-clean-CZ.txt",
        "adms": [
            ("province", "A.ADM1", "AddressProvince"),
            ("municipality", "A.ADM3", "AddressCity")
        ]
    }
}


