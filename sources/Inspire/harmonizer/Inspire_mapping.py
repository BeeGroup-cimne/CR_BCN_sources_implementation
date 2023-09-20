from ontology.namespaces_definition import Bigg, bigg_enums
from ontology.bigg_classes import LocationInfo, BuildingSpace, Device, UtilityPointOfDelivery
from slugify import slugify
from utils.data_transformations import *


class Mapping(object):

    def __init__(self, source, namespace):
        self.source = source
        BuildingSpace.set_namespace(namespace)
        UtilityPointOfDelivery.set_namespace(namespace)
        Device.set_namespace(namespace)

    def get_mappings(self, group):

        building_space = {
            "name": "building_space",
            "class": BuildingSpace,
            "type": {
                "origin": "row"
            },
            "params": {
                "mapping": {
                    "subject": {
                        "key": "building_space_subject",
                        "operations": []
                    }
                }
            },
            "links": {
                "device": {
                    "type": Bigg.isObservedByDevice,
                    "link": "CUPS"
                },
                "utility_point": {
                    "type": Bigg.hasUtilityPointOfDelivery,
                    "link": "CUPS"
                }
            }
        }

        utility_point = {
            "name": "utility_point",
            "class": UtilityPointOfDelivery,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasUtilityType": to_object_property("Gas", namespace=bigg_enums)
                },
                "mapping": {
                    "subject": {
                        "key": 'utility_point_subject',
                        "operations": []
                    },
                    "pointOfDeliveryIDFromOrganization": {
                        "key": 'utility_point_id',
                        "operations": []
                    },
                }
            },
            "links": {
                "device": {
                    "type": Bigg.hasDevice,
                    "link": "CUPS"
                }
            }
        }

        device = {
            "name": "device",
            "class": Device,
            "type": {
                "origin": "row"
            },
            "params": {
                "raw": {
                    "hasDeviceType": to_object_property("Meter.EnergyMeter.Gas", namespace=bigg_enums)
                },
                "mapping": {
                    "subject": {
                        "key": "device_subject",
                        "operations": []
                    },
                    "deviceName":  {
                        "key": 'CUPS',
                        "operations": []
                    }
                }
            }
        }

        grouped_modules = {
            "linked": [building_space, utility_point, device],
            "unlinked": [utility_point, device]
        }
        return grouped_modules[group]
