# Neighbourhoods

The neighbourhoods file contains the geometry information of barcelona city.

## Gathering tool

This data source comes in the format of an GPKG file.

#### RUN import application

To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so neighbourhoods -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format

The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will
have its own row key, that will be generated as follows:

#### Neighbourhoods

````json
{
  "codi_districte": 1,
  "nom_districte": "Ciutat Vella",
  "codi_barri": 1,
  "nom_barri": "el Raval",
  "geometria_etrs89": "POLYGON ((430164.372950341 4581940.39758424, 430105.024480832 4581881.93614338,))"
}

````

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Neighbourhoods=>

| Origin         | Harmonization                                    |
|----------------|--------------------------------------------------|
| codi_districte | districtId,  Relation (neighbourhoods, district) | 
| codi_barri     | s4city:Neighbourhood:neighbourhoodId             | 
| nom_barri      | s4city:Neighbourhood-officialName                | 


