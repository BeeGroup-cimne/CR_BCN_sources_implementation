# Census Tracts

The censusTracts file contains the census tracts, district and municipality geometry information of barcelona city.

## Gathering tool

This data source comes in the format of an GPKG file.

#### RUN import application

To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so census_tracts -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format

The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will
have its own row key, that will be generated as follows:

#### Census Tracts

````json
{
  "type": "Feature",
  "properties": {
    "fid": 1,
    "MUNICIPI": "080193",
    "DISTRICTE": "01",
    "SECCIO": "001",
    "MUNDISSEC": "08019301001"
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

#### Census Tract=>

| Origin    | Harmonization                    |
|-----------|----------------------------------|
| DISTRICTE | districtId                       | 
| SECCIO    | gn:parentADM5:censusTractId      | 
| MUNDISSEC | relation (censusTract, district) | 


