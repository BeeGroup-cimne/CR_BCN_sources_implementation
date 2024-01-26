import os
import numpy as np
import pandas as pd
import yaml
from processing.core.Processing import Processing
from qgis.analysis import QgsNativeAlgorithms
from qgis.core import *
from qgis.utils import *


def building_data_format_script(unzip_dir):
    unzip_dir = "data/Inspire/download/building/unzip"
    for root, dirs, files in os.walk(unzip_dir):
        print(root, dirs)
        print(files)
        buildings_list = []
        building_part_list = []
        for file in files:
            if file.endswith("building.gml"):
                # Building
                buildings_list.append(os.path.join(root, file))

            if file.endswith("buildingpart.gml"):
                # Buildingpart
                building_part_list.append(os.path.join(root, file))

        # Building
        Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': buildings_list,
                                                             'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                                                             'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_buildings.gpkg\' table="output" (geom)'})
        Processing.runAlgorithm("native:selectbylocation", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_buildings.gpkg|layername=output',
            'PREDICATE': [0],
            'INTERSECT': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_municipalities.geojson',
            'METHOD': 0})
        # export como gpkg 08900_buildings.gpkg
        Processing.runAlgorithm("qgis:exportaddgeometrycolumns", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings.gpkg|layername=08900_buildings',
            'CALC_METHOD': 0,
            'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom.gpkg\' table="output" (geom)'})
        # export geojson 08900_buildings_geom.geojson
        Processing.runAlgorithm("native:centroids", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom.gpkg|layername=output',
            'ALL_PARTS': True,
            'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid.geojson'})
        Processing.runAlgorithm("native:joinattributesbylocation", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid.geojson',
            'PREDICATE': [5],
            'JOIN': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/BCN_Barrios.gpkg|layername=barcelonaciutat_barris',
            'JOIN_FIELDS': ['codi_barri', 'nom_barri'], 'METHOD': 0, 'DISCARD_NONMATCHING': False, 'PREFIX': '',
            'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid_ba.gpkg\' table="output" (geom)'})
        Processing.runAlgorithm("native:joinattributesbylocation", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid_ba.gpkg|layername=output',
            'PREDICATE': [5],
            'JOIN': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_census_tracts.geojson',
            'JOIN_FIELDS': ['MUNDISSEC'], 'METHOD': 0, 'DISCARD_NONMATCHING': False, 'PREFIX': '',
            'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid_ba_ct.gpkg\' table="output" (geom)'})
        Processing.runAlgorithm("native:joinattributesbylocation", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid_ba_ct.gpkg|layername=output',
            'PREDICATE': [5],
            'JOIN': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_postal_codes.geojson',
            'JOIN_FIELDS': ['CODPOS'], 'METHOD': 0, 'DISCARD_NONMATCHING': False, 'PREFIX': '',
            'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid_ba_ct_pc.geojson'})

        # Buildingpart
        Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': building_part_list,
                                                             'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                                                             'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_building_part.gpkg\' table="output" (geom)'})
        Processing.runAlgorithm("native:selectbylocation", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_building_part.gpkg|layername=output',
            'PREDICATE': [0],
            'INTERSECT': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_municipalities.geojson',
            'METHOD': 0})
        # export como gpkg 08900_building_part.gpkg
        Processing.runAlgorithm("qgis:exportaddgeometrycolumns", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part.gpkg|layername=08900_building_part',
            'CALC_METHOD': 0,
            'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part_geom.gpkg\' table="output" (geom)'})
        # export geojson '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/08900_building_part_geom.geojson
        Processing.runAlgorithm("native:centroids", {
            'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part_geom.gpkg|layername=output',
            'ALL_PARTS': True,
            'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part_geom_centroid.geojson'})


def address_data_format_script(unzip_dir):
    unzip_dir = "data/Inspire/download/address/unzip"
    for root, dirs, files in os.walk(unzip_dir):
        print(root, dirs)
        print(files)
        address_th_list = []
        address_list = []
        for file in files:
            if file.endswith(".gml"):
                # Address Th
                address_th_list.append(os.path.join(root, file) + '|layername=ThoroughfareName')
                # Address
                address_list.append(os.path.join(root, file) + '|layername=Address')

    Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': address_th_list,
                                                         'CRS': None,
                                                         'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08_address_th.csv'})

    Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': address_list,
                                                         'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                                                         'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08_address.gpkg\' table="output" (geom)'})

    vlayer = QgsVectorLayer('/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08_address.gpkg|layername=output', 'layer', 'ogr')

    Processing.runAlgorithm("native:selectbylocation", {
        'INPUT': vlayer,
        'PREDICATE': [6],
        'INTERSECT': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_municipalities.geojson',
        'METHOD': 0})

    new_layer = vlayer.materialize(QgsFeatureRequest().setFilterFids(vlayer.selectedFeatureIds()))
    path = '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08900_address.gpkg'
    options = QgsVectorFileWriter.SaveVectorOptions()
    options.driverName = "GPKG"
    options.fileEncoding = "UTF-8"
    QgsVectorFileWriter.writeAsVectorFormatV3(new_layer, path, QgsCoordinateTransformContext(), options)

    project = QgsProject.instance()
    project.clear()
    a = QgsVectorLayer('/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08900_address.gpkg',
                       "GOOG",
                       "ogr")
    if not a.isValid():
        print("Layer failed to load address!")
    else:
        project.addMapLayer(a)

    project.setCrs(QgsCoordinateReferenceSystem(4326, QgsCoordinateReferenceSystem.EpsgCrsId))

    Processing.runAlgorithm("qgis:exportaddgeometrycolumns", {
        'INPUT': project.mapLayersByName('GOOG')[0],
        'CALC_METHOD': 1,
        'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/address/temp/08900_address_geom.geojson'})


if __name__ == '__main__':
    qgs = QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()
    qgs.processingRegistry().addProvider(QgsNativeAlgorithms())
    building_data_format_script("data/Inspire/download/building/unzip")
    address_data_format_script("data/Inspire/download/address/unzip")
