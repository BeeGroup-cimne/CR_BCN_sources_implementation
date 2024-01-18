# Datadis consumption

The Datadis consumption contains the energy electricity consumption for each postal code.

## Gathering tool

This data source comes in the format of an CSV file where there are columns that contains consumptions for a cadastral
reference.

#### RUN import application

To run the import application, execute the python script with the following parameters:

```bash
python3 -m gather -so datadis -f <file> -n <namespace> -u <user_importing> -tz <file_timezone> -st <storage>
```

## Raw Data Format

The data imported will be stored in the Hbase table, each endpoint that provides a different kind of information will
have its own row key, that will be generated as follows:

#### Datadis Consumptions

````json
{
}
````

## Harmonization

The harmonization of the data will be done with the following mapping:

#### Consumtion=>

| Origin | Harmonization |
|--------|---------------|
|        |               | 



