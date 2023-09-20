# Inspire description
Inspire(Cadaster) is a data source containing points and polygons in geojson format.

## Gathering tool
This data source comes in the format of an GeoJSON file where there are two values for each building, the first one have the building centroid and the second one contain the polygon GeoJSON and all the information about the building.

#### RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so Inspire -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format
The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will have its own  row key, that will be generated as follows:

 | class       | Hbase key |
|-------------|-----------|
| Measurement |           |

*Mapping key for Inspire source, where `ts_ini` is the starting date of the invoice*

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Building=>
| Origin | Harmonization |
|--------|---------------|
 |        |               | 




`