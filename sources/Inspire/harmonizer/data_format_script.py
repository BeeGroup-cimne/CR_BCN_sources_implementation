import os
import numpy as np
import pandas as pd
import yaml
from processing.core.Processing import Processing
from qgis.analysis import QgsNativeAlgorithms
from qgis.core import *
from qgis.utils import *


def data_format_script(unzip_dir):
    unzip_dir = "data/Inspire/download/building/unzip"
    dir_path = "/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/unzip"
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
        print(building_part_list)
        # # Building
        # Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': buildings_list,
        #                                                      'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
        #                                                      'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_buildings.gpkg\' table="output" (geom)'})
        # Processing.runAlgorithm("native:selectbylocation", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_buildings.gpkg|layername=output',
        #     'PREDICATE': [0],
        #     'INTERSECT': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_municipalities.geojson',
        #     'METHOD': 0})
        # # export como gpkg 08900_buildings.gpkg
        # Processing.runAlgorithm("qgis:exportaddgeometrycolumns", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings.gpkg|layername=08900_buildings',
        #     'CALC_METHOD': 0,
        #     'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom.gpkg\' table="output" (geom)'})
        # # export geojson 08900_buildings_geom.geojson
        # Processing.runAlgorithm("native:centroids", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom.gpkg|layername=output',
        #     'ALL_PARTS': True,
        #     'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom_centroid.geojson'})

        # Buildingpart
        # Processing.runAlgorithm("native:mergevectorlayers", {'LAYERS': building_part_list,
        #                                                      'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
        #                                                      'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_building_part.gpkg\' table="output" (geom)'})
        # Processing.runAlgorithm("native:selectbylocation", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08_building_part.gpkg|layername=output',
        #     'PREDICATE': [0],
        #     'INTERSECT': '/home/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/bcn_municipalities.geojson',
        #     'METHOD': 0})
        # # export como gpkg 08900_building_part.gpkg
        # Processing.runAlgorithm("qgis:exportaddgeometrycolumns", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part.gpkg|layername=08900_building_part',
        #     'CALC_METHOD': 0,
        #     'OUTPUT': 'ogr:dbname=\'/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_building_part_geom.gpkg\' table="output" (geom)'})
        # # export geojson '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/08900_building_part_geom.geojson
        # Processing.runAlgorithm("native:centroids", {
        #     'INPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/temp/08900_buildings_geom.gpkg|layername=output',
        #     'ALL_PARTS': True,
        #     'OUTPUT': '/home/jose/CR_BCN_sources_implementation/data/Inspire/download/building/08900_buildings_geom_centroid.geojson'})


if __name__ == '__main__':
    qgs = QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()
    qgs.processingRegistry().addProvider(QgsNativeAlgorithms())
    data_format_script("data/Inspire/download/building/unzip")

# # Building 08011
# processing.run("qgis:exportaddgeometrycolumns", {'INPUT': QgsProcessingFeatureSourceDefinition(
#     '/vsizip//Users/jose/Downloads/A.ES.SDGC.BU.08900.zip/A.ES.SDGC.BU.08900.building.gml|layername=Building|geometrytype=Polygon|uniqueGeometryType=yes',
#     selectedFeaturesOnly=False, featureLimit=-1,
#     flags=QgsProcessingFeatureSourceDefinition.FlagOverrideDefaultGeometryCheck,
#     geometryCheck=QgsFeatureRequest.GeometrySkipInvalid), 'CALC_METHOD': 0, 'OUTPUT': 'TEMPORARY_OUTPUT'})
# processing.run("native:selectbylocation", {
#     'INPUT': '/vsizip//Users/jose/Downloads/A.ES.SDGC.BU.08900.zip/A.ES.SDGC.BU.08900.building.gml|layername=Building|geometrytype=Polygon|uniqueGeometryType=yes',
#     'PREDICATE': [6],
#     'INTERSECT': '/Users/jose/Downloads/codis postals.gpkg|layername=spanish_postal_codes_validated|subset="CODPOS" = \'08011\'',
#     'METHOD': 0})
# export
# geojson
#
# processing.run("native:centroids", {'INPUT': QgsProcessingFeatureSourceDefinition(
#     'Polygon?crs=EPSG:25831&field=gml_id:string(0,0)&field=beginLifespanVersion:string(19,0)&field=conditionOfConstruction:string(10,0)&field=beginning:string(19,0)&field=end:string(19,0)&field=endLifespanVersion:string(19,0)&field=informationSystem:string(92,0)&field=reference:string(14,0)&field=localId:string(14,0)&field=namespace:string(10,0)&field=horizontalGeometryEstimatedAccuracy:double(0,0)&field=horizontalGeometryEstimatedAccuracy_uom:string(1,0)&field=horizontalGeometryReference:string(9,0)&field=referenceGeometry:boolean(0,0)&field=currentUse:string(18,0)&field=numberOfBuildingUnits:integer(0,0)&field=numberOfDwellings:integer(0,0)&field=numberOfFloorsAboveGround:string(0,0)&field=documentLink:string(128,0)&field=format:string(4,0)&field=sourceStatus:string(11,0)&field=officialAreaReference:string(14,0)&field=value:integer(0,0)&field=value_uom:string(2,0)&field=area:double(0,0)&field=perimeter:double(0,0)&uid={a89fbc9d-cabd-486f-bf26-a301ea1fa955}',
#     selectedFeaturesOnly=False, featureLimit=-1,
#     flags=QgsProcessingFeatureSourceDefinition.FlagOverrideDefaultGeometryCheck,
#     geometryCheck=QgsFeatureRequest.GeometrySkipInvalid), 'ALL_PARTS': False, 'OUTPUT': 'TEMPORARY_OUTPUT'})
#
# processing.run("native:selectbylocation", {
#     'INPUT': 'Point?crs=EPSG:25831&field=gml_id:string(0,0)&field=beginLifespanVersion:string(19,0)&field=conditionOfConstruction:string(10,0)&field=beginning:string(19,0)&field=end:string(19,0)&field=endLifespanVersion:string(19,0)&field=informationSystem:string(92,0)&field=reference:string(14,0)&field=localId:string(14,0)&field=namespace:string(10,0)&field=horizontalGeometryEstimatedAccuracy:double(0,0)&field=horizontalGeometryEstimatedAccuracy_uom:string(1,0)&field=horizontalGeometryReference:string(9,0)&field=referenceGeometry:boolean(0,0)&field=currentUse:string(18,0)&field=numberOfBuildingUnits:integer(0,0)&field=numberOfDwellings:integer(0,0)&field=numberOfFloorsAboveGround:string(0,0)&field=documentLink:string(128,0)&field=format:string(4,0)&field=sourceStatus:string(11,0)&field=officialAreaReference:string(14,0)&field=value:integer(0,0)&field=value_uom:string(2,0)&field=area:double(0,0)&field=perimeter:double(0,0)&uid={67850d1b-d5e0-4cfe-9081-92d975002a11}',
#     'PREDICATE': [0],
#     'INTERSECT': '/Users/jose/Downloads/codis postals.gpkg|layername=spanish_postal_codes_validated|subset="CODPOS" = \'08011\'',
#     'METHOD': 0})
# Export
# geojson
# centroid
# # buidingpart
#
# processing.run("qgis:exportaddgeometrycolumns", {'INPUT': QgsProcessingFeatureSourceDefinition(
#     '/vsizip//Users/jose/Downloads/A.ES.SDGC.BU.08900.zip/A.ES.SDGC.BU.08900.buildingpart.gml|layername=BuildingPart',
#     selectedFeaturesOnly=False, featureLimit=-1,
#     flags=QgsProcessingFeatureSourceDefinition.FlagOverrideDefaultGeometryCheck,
#     geometryCheck=QgsFeatureRequest.GeometrySkipInvalid), 'CALC_METHOD': 0, 'OUTPUT': 'TEMPORARY_OUTPUT'})
#
# processing.run("native:selectbylocation", {
#     'INPUT': 'Polygon?crs=EPSG:25831&field=gml_id:string(0,0)&field=beginLifespanVersion:string(19,0)&field=conditionOfConstruction:string(0,0)&field=localId:string(22,0)&field=namespace:string(10,0)&field=horizontalGeometryEstimatedAccuracy:double(0,0)&field=horizontalGeometryEstimatedAccuracy_uom:string(1,0)&field=horizontalGeometryReference:string(9,0)&field=referenceGeometry:boolean(0,0)&field=numberOfFloorsAboveGround:integer(0,0)&field=heightBelowGround:integer(0,0)&field=heightBelowGround_uom:string(1,0)&field=numberOfFloorsBelowGround:integer(0,0)&field=area:double(0,0)&field=perimeter:double(0,0)&uid={dc0a8acf-d98c-4451-9df3-b23ba43d6089}',
#     'PREDICATE': [6],
#     'INTERSECT': '/Users/jose/Downloads/codis postals.gpkg|layername=spanish_postal_codes_validated|subset="CODPOS" = \'08011\'',
#     'METHOD': 0})
# # exportgeojson
# processing.run("native:centroids", {'INPUT': QgsProcessingFeatureSourceDefinition(
#     '/Users/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/data/Inspire/buildingSpace/08011buildingSpaceGeoJson.geojson',
#     selectedFeaturesOnly=False, featureLimit=-1,
#     flags=QgsProcessingFeatureSourceDefinition.FlagOverrideDefaultGeometryCheck,
#     geometryCheck=QgsFeatureRequest.GeometrySkipInvalid), 'ALL_PARTS': False,
#     'OUTPUT': '/Users/jose/Nextcloud/Beegroup/Projects/ClimateReady-BCN/Dades/temp/data/Inspire/buildingSpace/08011buildingSpaceCentroid.geojson'})
# # exportcentroid
#
# # address
#
# processing.run("native:selectbylocation", {
#     'INPUT': '/vsizip//Users/jose/Downloads/A.ES.SDGC.AD.08900.zip/A.ES.SDGC.AD.08900.gml|layername=Address',
#     'PREDICATE': [6],
#     'INTERSECT': '/Users/jose/Downloads/codis postals.gpkg|layername=spanish_postal_codes_validated|subset="CODPOS" = \'08011\'',
#     'METHOD': 0})
# #export address08011
# #export Thoroughfare 08900 to csv data/Inspire/address/addressTh08900.csv (con esto podemos hacer match entre el Th y las addres y conseguir la calle.)
