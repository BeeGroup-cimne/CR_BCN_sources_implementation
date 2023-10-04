# Inspire description
Inspire(Cadaster) is a data source containing points and polygons in geojson format.

## Gathering tool
This data source comes in the format of an GeoJSON file where there are two values for each building, first one have the building centroid, the second one contain the polygon GeoJSON and all the information about the building.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Inspire -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format
The data imported will be stored:
#### Buidings
````json
{
    "buildings": {
        "point": [
            { "type": "Feature",
                "properties": { 
                    "gml_id": "ES.SDGC.BU.000400300DF38H", 
                    "beginLifespanVersion": "2007-04-23T00:00:00", 
                    "conditionOfConstruction": "functional", 
                    "beginning": "1933-01-01T00:00:00", 
                    "end": "1933-01-01T00:00:00", 
                    "endLifespanVersion": null, 
                    "informationSystem": "https://www1.sedecatastro.gob.es/CYCBienInmueble/OVCListaBienes.aspx?rc1=0004003&rc2=00DF38H", 
                    "reference": "000400300DF38H", 
                    "localId": "000400300DF38H", 
                    "namespace": "ES.SDGC.BU", 
                    "horizontalGeometryEstimatedAccuracy": 0.1, 
                    "horizontalGeometryEstimatedAccuracy_uom": "m", 
                    "horizontalGeometryReference": "footPrint", 
                    "referenceGeometry": true, 
                    "currentUse": null, 
                    "numberOfBuildingUnits": 1, 
                    "numberOfDwellings": 1, 
                    "numberOfFloorsAboveGround": null, 
                    "documentLink": "http://ovc.catastro.meh.es/OVCServWeb/OVCWcfLibres/OVCFotoFachada.svc/RecuperarFotoFachadaGet?ReferenciaCatastral=000400300DF38H", 
                    "format": "jpeg", 
                    "sourceStatus": "NotOfficial", 
                    "officialAreaReference": "grossFloorArea", 
                    "value": 40, 
                    "value_uom": "m2", 
                    "area": 31.53759765625, 
                    "perimeter": 25.729075441229419
                },
                "geometry": { 
                    "type": "Point", 
                    "coordinates": "[ 438092.016499999852385, 4589396.840500000864267 ]"
                }
            }
        ],
        "geojson": [
            { 
                "type": "Feature",
                "properties": { 
                    "gml_id": "ES.SDGC.BU.000400300DF38H", 
                    "beginLifespanVersion": "2007-04-23T00:00:00", 
                    "conditionOfConstruction": "functional", 
                    "beginning": "1933-01-01T00:00:00", 
                    "end": "1933-01-01T00:00:00", 
                    "endLifespanVersion": null, 
                    "informationSystem": "https://www1.sedecatastro.gob.es/CYCBienInmueble/OVCListaBienes.aspx?rc1=0004003&rc2=00DF38H", 
                    "reference": "000400300DF38H", 
                    "localId": "000400300DF38H", 
                    "namespace": "ES.SDGC.BU", 
                    "horizontalGeometryEstimatedAccuracy": 0.1, 
                    "horizontalGeometryEstimatedAccuracy_uom": "m", 
                    "horizontalGeometryReference": "footPrint", 
                    "referenceGeometry": true, 
                    "currentUse": null, 
                    "numberOfBuildingUnits": 1, 
                    "numberOfDwellings": 1, 
                    "numberOfFloorsAboveGround": null, 
                    "documentLink": "http://ovc.catastro.meh.es/OVCServWeb/OVCWcfLibres/OVCFotoFachada.svc/RecuperarFotoFachadaGet?ReferenciaCatastral=000400300DF38H", 
                    "format": "jpeg", 
                    "sourceStatus": "NotOfficial", 
                    "officialAreaReference": "grossFloorArea", 
                    "value": 40, 
                    "value_uom": "m2", 
                    "area": 31.53759765625, 
                    "perimeter": 25.729075441229419
                },
                "geometry": { 
                    "type": "Polygon", 
                    "coordinates": "[ [ [ 438087.647, 4589399.393000000156462 ], [ 438090.146, 4589401.542000000365078 ], [ 438096.386, 4589394.287999999709427 ], [ 438093.886999999987893, 4589392.139000000432134 ], [ 438087.647, 4589399.393000000156462 ] ] ]"
                }
            }
        ]
    }
}
````
#### BuildingSpace
````json
{
    "buildingsSpaces": {
        "punto": [
            {
                "type": "Feature",
                "properties": {
                    "gml_id": "ES.SDGC.BU.000400400DF38H_part1",
                    "beginLifespanVersion": "2007-04-23T00:00:00",
                    "conditionOfConstruction": null,
                    "localId": "000400400DF38H_part1",
                    "namespace": "ES.SDGC.BU",
                    "horizontalGeometryEstimatedAccuracy": 0.1,
                    "horizontalGeometryEstimatedAccuracy_uom": "m",
                    "horizontalGeometryReference": "footPrint",
                    "referenceGeometry": true,
                    "numberOfFloorsAboveGround": 1,
                    "heightBelowGround": 0,
                    "heightBelowGround_uom": "m",
                    "numberOfFloorsBelowGround": 0,
                    "area": 22.7852783203125,
                    "perimeter": 20.111491819503449
                },
                "geometry": { 
                    "type": "Point", 
                    "coordinates": "[ 438086.824925219058059, 4589409.676963367499411 ]"
                }
            }
        ],
        "geojson": [
            {
                "type": "Feature",
                "properties": {
                    "gml_id": "ES.SDGC.BU.000400400DF38H_part1",
                    "beginLifespanVersion": "2007-04-23T00:00:00",
                    "conditionOfConstruction": null,
                    "localId": "000400400DF38H_part1",
                    "namespace": "ES.SDGC.BU",
                    "horizontalGeometryEstimatedAccuracy": 0.1,
                    "horizontalGeometryEstimatedAccuracy_uom": "m",
                    "horizontalGeometryReference": "footPrint",
                    "referenceGeometry": true,
                    "numberOfFloorsAboveGround": 1,
                    "heightBelowGround": 0,
                    "heightBelowGround_uom": "m",
                    "numberOfFloorsBelowGround": 0,
                    "area": 22.7852783203125,
                    "perimeter": 20.111491819503449
                },
                "geometry": { 
                    "type": "Polygon", 
                    "coordinates": "[ [ [ 438087.672000000020489, 4589406.048000000417233 ], [ 438083.363499999977648, 4589411.057 ], [ 438085.977499999979045, 4589413.30599999986589 ], [ 438090.286499999987427, 4589408.29700000025332 ], [ 438087.672000000020489, 4589406.048000000417233 ] ] ]"
                }
            }
        ]
    }
}
````

#### Address
````json
{
    "address": {
        "punto": [
            {
                "type": "Feature",
                "properties": {
                    "gml_id": "ES.SDGC.AD.08.015.1147.S-N.000400300DF38H",
                    "localId": "08.015.1147.S-N.000400300DF38H",
                    "namespace": "ES.SDGC.AD",
                    "specification": "Parcel",
                    "method": "fromFeature",
                    "default": true,
                    "designator": "S-N",
                    "type": 1,
                    "level": "siteLevel",
                    "validFrom": null,
                    "beginLifespanVersion": "2007-04-23T00:00:00"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": "[ 438092.01, 4589395.339999999850988 ]"
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "gml_id": "ES.SDGC.AD.08.015.1147.S-N.000400400DF38H",
                    "localId": "08.015.1147.S-N.000400400DF38H",
                    "namespace": "ES.SDGC.AD",
                    "specification": "Parcel",
                    "method": "fromFeature",
                    "default": true,
                    "designator": "S-N",
                    "type": 1,
                    "level": "siteLevel",
                    "validFrom": null,
                    "beginLifespanVersion": "2007-04-23T00:00:00"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": "[ 438093.88, 4589399.78000000026077 ]"
                }
            }
        ]
    }
}

````


## Harmonization

The harmonization of the data will be done with the following mapping:

#### Building=>
| Origin                  | Harmonization                       |
|-------------------------|-------------------------------------|
 | beginLifespanVersion    | s4bldg:Building-startState          | 
| conditionOfConstruction | s4bldg:Building-state               |
| beginning               | s4bldg:Building-startConstruction   |
| end                     | s4bldg:Building-endConstruction     |
| endLifespanVersion      | s4bldg:Building-endState            |
| informationSystem       | s4bldg:Building-cadasterDetailedUrl |
| reference               | s4bldg:Building-cadastralId         |
| currentUse              | s4bldg:Building-mainUse             |
| numberOfBuildingsUnits  | s4bldg:Building-buildingUnits       |
| numberOfDwellings       | s4bldg:Building-dwellings           |
| officialAreaReference   | saref:Measurement-type              |
| value                   | saref:Measurement-value             |
| value_uom               | saref:UnitOfMeasure-unit            |

#### BuildingSpace=>
| Origin                    | Harmonization                                   |
|---------------------------|-------------------------------------------------|
 | localId                   | s4bldg:BuildingSpace-CadastralLocalId           | 
 | numberOfFloorsAboveGround | s4bldg:BuildingSpace-numberOfFloorsAboveGround  | 
 | numberOfFloorsBelowGround | s4bldg:BuildingSpace-numberOfFloorsBellowGround | 
 | area                      | s4bldg:BuildingSpace-builtUpLandArea            | 
 
#### Address=>
| Origin               | Harmonization               |
|----------------------|-----------------------------|
 | localId              | vcard:Address-addressId     | 
 | specification        | vcard:Address-specification | 
 | designator           | vcard:Address-streetNumber  | 
 | streetname           | vcard:Address-streetName    | 
 | beginLifespanVersion | vcard:Address-startState    | 

