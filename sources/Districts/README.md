# Municipalities

The Municipalities file contains the municipalities geometry information of barcelona city.

## Gathering tool

This data source comes in the format of an GPKG file.

#### RUN import application

To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so municipalities -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format

The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will
have its own row key, that will be generated as follows:

#### Municipalities

````json
{
  "type": "Feature",
  "properties": {
    "CODIMUNI": "080193",
    "NOMMUNI": "Barcelona",
    "AREAOFI": null,
    "AREAPOL": 101.7629,
    "CODICOMAR": "13",
    "CODIPROV": "08",
    "VALIDDE": null,
    "DATAALTA": "202001011200"
  },
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": [
      [
        [
          [
            2.178545014164631,
            41.37480008768268
          ],
          [
            2.17797105321016,
            41.37408466496421
          ]
        ]
      ]
    ]
  }
}
````

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Building=>

| Origin    | Harmonization                                               |
|-----------|-------------------------------------------------------------|
| MUNICIPI  | municipalityId                                              | 
| DISTRICTE | gn:parentADM4:districtId, Relation (municipality, district) |


