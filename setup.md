# Install Neo4j

Follow the instruction in the [link](https://neo4j.com/docs/operations-manual/current/installation/linux/debian/#debian-installation)

Add in the `plugins` directory located at `/var/lib/neo4j/plugins`:
 - the neosemantics
 - the apoc

if you face the error `NoSuchMethodError` with `apoc.convert.fromJsonList`, follow the instruction in the [link](https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/2861)

# Set up the database (first time)
- login and change the password.
- run in neo4j:
```cypher 
CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE
```
# Set up the database (all the time after reset)
- run in neo4j:
```cypher
CALL n10s.graphconfig.init({ keepLangTag: true, handleMultival:"ARRAY", multivalPropList:["http://bigg-project.eu/ontology#kpiType","http://bigg-project.eu/ontology#shortName","http://www.w3.org/2000/01/rdf-schema#label", "http://www.w3.org/2000/01/rdf-schema#comment", "http://www.geonames.org/ontology#officialName"]});
CALL n10s.nsprefixes.add("bigg","http://bigg-project.eu/ontology#");
CALL n10s.nsprefixes.add("geo","http://www.geonames.org/ontology#");
CALL n10s.nsprefixes.add("unit","http://qudt.org/vocab/unit/");
CALL n10s.nsprefixes.add("qudt","http://qudt.org/schema/qudt/");
CALL n10s.nsprefixes.add("wgs","http://www.w3.org/2003/01/geo/wgs84_pos#");
CALL n10s.nsprefixes.add("rdfs","http://www.w3.org/2000/01/rdf-schema#");
```
* add other namespaces if required.

# LOAD DATA FOR BIGG

## 1. Load the taxonomies
Create the dictionaries, when they are already translated
```bash
echo "add translation to previously created dictionaries"
python3 -m set_up.Dictionaries -a load
```

<details>
  <summary>set up the translation files</summary>

```bash
echo "create dictionaries without translation"
python3 -m set_up.Dictionaries -a load_translate(deprecated)
echo "create translation files for the taxonomies"
python3 -m set_up.Dictionaries -a create(deprecated)
echo "add translation to previously created dictionaries"
python3 -m set_up.Dictionaries -a translate(deprecated)
```
</details>


## 2. Load weather stations
```bash
echo "weather stations catalonia"
python3 -m set_up.Weather -f data/Weather/cpcat.json -cn "ES" -n "https://weather.beegroup-cimne.com#" -c
echo "weather stations bulgaria"
python3 -m set_up.Weather -f data/Weather/locbg.json -cn "BG" -n "https://weather.beegroup-cimne.com#" -c
```
<details>
  <summary>Weather stations file format</summary>

Weather station file should be in the form {"name":["lat", "lon"], ...}
</details>

----
## 3. Icaen Organization 

arguments:
 - namespace: `https://icaen.cat#`
 - username: `icaen`

<details>
    <summary>Load icaen data</summary>

### 3.1 Set up the organization and data sources
```bash
echo "org"
python3 -m set_up.Organizations -f data/Organizations/gencat-organizations2.xls -name "Generalitat de Catalunya" -u "icaen" -n "https://icaen.cat#"
echo "Gemweb source"
python3 -m set_up.DataSources -u "icaen" -n "https://icaen.cat#" -f data/DataSources/gemweb.xls -d GemwebSource
echo "datadis source"
python3 -m set_up.DataSources -u "icaen" -n "https://icaen.cat#" -f data/DataSources/datadis.xls -d DatadisSource
echo "nedgia source"
python3 -m set_up.DataSources -u "icaen" -n "https://icaen.cat#" -f data/DataSources/nedgia.xls -d NedgiaSource
echo "simpleTariff source"
python3 -m set_up.DataSources -u "icaen" -n "https://icaen.cat#" -f data/DataSources/simpleTariff.xls -d SimpleTariffSource
echo "co2Emisions source"
python3 -m set_up.DataSources -u "icaen" -n "https://icaen.cat#" -f data/DataSources/simpleTariff.xls -d CO2EmissionsSource
```

### 3.2. Harmonize the static data

Load from HBASE (recomended when re-harmonizing)

```bash
echo "GPG"
python3 -m harmonizer -so GPG -u "icaen" -n "https://icaen.cat#" -o -c
echo "Gemweb"
python3 -m harmonizer -so Gemweb -u "icaen" -n "https://icaen.cat#" -c
echo "Genercat"
python3 -m harmonizer -so Genercat -u "icaen" -n "https://icaen.cat#" -c
echo "Datadis static"
python3 -m harmonizer -so Datadis -n "https://icaen.cat#" -u icaen -t static -c
python3 -m harmonizer -so CEEC3X -n "https://icaen.cat#" -u icaen -c
python3 -m harmonizer -so OpenData -n "https://icaen.cat#" -u icaen -c
```

<details>
  <summary>Load from KAFKA (online harmonization)</summary>

1. start the harmonizer and store daemons:
```bash
python3 -m harmonizer
python3 -m store
```

2. Launch the gather utilities

```bash
python3 -m gather -so GPG -f "data/GPG/2022-10 SIME-DadesdelsImmobles v2.xlsx" -n "https://icaen.cat#" -st kafka -u icaen
python3 -m gather -so Gemweb -st kafka
python3 -m gather -so Genercat -f data/genercat/data2.xls -u icaen -n "https://icaen.cat#" -st kafka
python3 -m gather -so CEEC3X -f "data/CEEC3X/ceec3x-01639-2TX229LJ9.xml" -b 01639 -id 2TX229LJ9 -n "https://icaen.cat#" -u icaen  -st kafka

python3 -m gather -so Datadis # MR-Job
python3 -m gather -so Weather # MR-Job
python3 -m gather -so OpenData -n "https://icaen.cat#" -u icaen -st kafka

```
</details>


### 3.3. Create a new Tariff and co2Emissions for the organization
The creation queries are made custom or manually or by the UI

```cypher
Match (o:bigg__Organization{userID:"icaen"})
Match (s:SimpleTariffSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__Tariff:Resource{bigg__tariffCompany:"CIMNE", bigg__tariffName: "electricdefault", uri: "https://icaen.cat#TARIFF-SimpleTariffSource-icaen-electricdefault"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"icaen"})
Match (s:SimpleTariffSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__Tariff:Resource{bigg__tariffCompany:"CIMNE", bigg__tariffName: "gasdefault", uri: "https://icaen.cat#TARIFF-SimpleTariffSource-icaen-gasdefault"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"icaen"})
Match (s:CO2EmissionsSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__CO2EmissionsFactor:Resource{bigg__CO2EmissionsStation:"cataloniaElectric", wgs__lat:40.959, wgs__lon:1.485, uri: "https://icaen.cat#CO2EMISIONS-cataloniaElectric"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"icaen"})
Match (s:CO2EmissionsSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__CO2EmissionsFactor:Resource{bigg__CO2EmissionsStation:"cataloniaGas", wgs__lat:40.959, wgs__lon:1.485, uri: "https://icaen.cat#CO2EMISIONS-cataloniaGas"})-[:importedFromSource]->(s)
return t;
```
### 3.4. Link all buildings to tariff and CO2Emissions
The creation queries are made custom or manually or by the UI

```
Match (bigg__Organization{userID:"icaen"})-[:hasSource]->(:SimpleTariffSource)<-[:importedFromSource]-(t:bigg__Tariff{bigg__tariffName:"electricdefault"})
Match (dt {uri:"http://bigg-project.eu/ontology#Electricity"})
Match (bigg__Organization{userID:"icaen"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (c:bigg__ContractedTariff:Resource{bigg__contractStartDate: datetime("2000-01-01T00:00:00.000+0100"), bigg__contractName:"electricdefault", uri: s.uri+"_tariff"})
Merge (s)-[:bigg__hasContractedTariff]->(c)
Merge (c)-[:bigg__hasTariff]->(t)
return t;

Match (bigg__Organization{userID:"icaen"})-[:hasSource]->(:SimpleTariffSource)<-[:importedFromSource]-(t:bigg__Tariff{bigg__tariffName:"gasdefault"})
Match (dt {uri:"http://bigg-project.eu/ontology#Gas"})
Match (bigg__Organization{userID:"icaen"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (c:bigg__ContractedTariff:Resource{bigg__contractStartDate: datetime("2000-01-01T00:00:00.000+0100"), bigg__contractName:"gasdefault", uri: s.uri+"_tariff"})
Merge (s)-[:bigg__hasContractedTariff]->(c)
Merge (c)-[:bigg__hasTariff]->(t)
return t;

Match (bigg__Organization{userID:"icaen"})-[:hasSource]->(:CO2EmissionsSource)<-[:importedFromSource]-(co2:bigg__CO2EmissionsFactor{bigg__CO2EmissionsStation:"cataloniaElectric"})
Match (dt {uri:"http://bigg-project.eu/ontology#Electricity"})
Match (bigg__Organization{userID:"icaen"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (s)-[:bigg__hasCO2EmissionsFactor]->(co2)
return co2;

Match (bigg__Organization{userID:"icaen"})-[:hasSource]->(:CO2EmissionsSource)<-[:importedFromSource]-(co2:bigg__CO2EmissionsFactor{bigg__CO2EmissionsStation:"cataloniaGas"})
Match (dt {uri:"http://bigg-project.eu/ontology#Gas"})
Match (bigg__Organization{userID:"icaen"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (s)-[:bigg__hasCO2EmissionsFactor]->(co2)
return co2;
```

### 3.5. Load tariff and co2 timeseries
```bash
python3 -m harmonizer -so SimpleTariff -u icaen -mp "http://bigg-project.eu/ontology#Price" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -c 
python3 -m harmonizer -so SimpleTariff -u icaen -mp "http://bigg-project.eu/ontology#Price" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -c 
python3 -m harmonizer -so CO2Emissions -u icaen -mp "http://bigg-project.eu/ontology#CO2Emissions" -p "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -pu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -c 
python3 -m harmonizer -so CO2Emissions -u icaen -mp "http://bigg-project.eu/ontology#CO2Emissions" -p "http://bigg-project.eu/ontology#EnergyConsumptionGas" -pu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -c 
```

<details>
<summary>Load from KAFKA</summary>

1. start the harmonizer and store daemons:
```bash
python3 -m harmonizer
python3 -m store
```

2. Launch the gather utilities

```bash
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_ELECSP_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 --co2_uid cataloniaElectric -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -st kafka 
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_GASNAT_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 --co2_uid cataloniaGas -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://bigg-project.eu/ontology#KiloGM-CO2" -n "https://icaen.cat#" -st kafka 
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_ELEC_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 -tar electricdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGridElectricity" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -st kafka
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_GASNAT_test01.xlsx -u icaen -di 2015-01-01 -de 2030-01-01 -tar gasdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGas" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://icaen.cat#" -st kafka 

```

</details>

### 3.6. Link building with the closest Weather Station
```bash
echo "Link WS with Buildings"
python3 -m set_up.Weather -cn "ES" -f data/Weather/cpcat.json -n "https://weather.beegroup-cimne.com#" -u
```

### 3.7. Load Timeseries Data

Fast-Load TS (recommended when re-harmonize)
```bash
echo "Datadis TS"
python3 -m harmonizer -so Datadis -n "https://icaen.cat#" -u icaen -t fast-ts -c
echo "Nedgia"
python3 -m harmonizer -so Nedgia -n "https://icaen.cat#" -u icaen -tz "Europe/Madrid" -t fast-ts -c
echo "Weather ts"
python3 -m harmonizer -so Weather -n "https://weather.beegroup-cimne.com#" -t fast-ts -c
```

<details>
 <summary> Load TS (harmonize full timeseries)</summary>

```bash
echo "Datadis TS"
python3 -m harmonizer -so Datadis -n "https://icaen.cat#" -u icaen -t ts -c
echo "Nedgia"
python3 -m harmonizer -so Nedgia -n "https://icaen.cat#" -u icaen -tz "Europe/Madrid" -t ts -c
echo "Weather ts"
python3 -m harmonizer -so Weather -n "https://weather.beegroup-cimne.com#" -t ts -c
```

</details>

### 3.8. Create Device AGGREGATORS

```bash
echo "DeviceAggregators datadis"
python3 -m set_up.DeviceAggregator -cn "ES" -t "totalElectricityConsumption"
echo "DeviceAggregators nedgia"
python3 -m set_up.DeviceAggregator -cn "ES" -t "totalGasConsumption"
echo "DeviceAggregators weather"
python3 -m set_up.DeviceAggregator -cn "ES" -t "externalWeather"
```

### 3.9. Create the Generic Buildings

```cypher
Match(n{userID:"icaen"})-[:bigg__hasSubOrganization]->(o{bigg__organizationDivisionType:"Department"})-[:bigg__hasSubOrganization*]->(t{bigg__organizationDivisionType:"Department"}) 
with t 
Match(ut{uri:"http://bigg-project.eu/ontology#Unknown"})
Match(at{uri:"http://bigg-project.eu/ontology#GrossFloorArea"})
Match(atu{uri:"http://qudt.org/vocab/unit/M2"})
Match(lc{uri:"https://sws.geonames.org/6356051/"})
Match(lp{uri:"https://sws.geonames.org/3128759/"})
With t, ut, at,atu

Create(ob:bigg__Organization:Resource:bigg_Thing{bigg__organizationName: "Generic("+t.bigg__organizationName+")", bigg__organizationDivisionType:"Building", generic:1,
uri:"https://icaen.cat#ORGANIZATION-FE"+id(t)}) 
Create(gb:bigg__Building:Resource:bigg_Thing{bigg__buildingIDFromOrganization:"FE"+id(t), bigg__buildingName:"Generic("+t.bigg__organizationName+")", generic:1,
uri:"https://icaen.cat#BUILDING-FE"+id(t)}) 
Create(s:bigg__BuildingSpace:Resource:bigg_Thing{bigg__buildingSpaceName: "Building", generic:1,
uri:"https://icaen.cat#BUILDINGSPACE-FE"+id(t)}) 
Create(a:bigg__Area:Resource:bigg_Thing{bigg__areaValue: "0", generic:1, uri:"https://icaen.cat#AREA-GrossFloorArea-generic-FE"+id(t)}) 
Create(e:bigg__BuildingElement:bigg__BuildingConstructionElement:Resource:bigg_Thing{generic:1, uri:"https://icaen.cat#ELEMENT-FE"+id(t)}) 
Create(bl:bigg__LocationInfo:Resource:bigg_Thing{generic:1, bigg__addressPostalCode:"0000", bigg__addressStreetName:"generic", bigg__addressStreetNumber:"0",
uri:"https://icaen.cat#LOCATION-FE"+id(t)}) 

Merge(t)-[:bigg__hasSubOrganization]->(ob)
Merge(ob)-[:bigg__managesBuilding]->(gb)
Merge(gb)-[:bigg__hasLocationInfo]->(bl)
Merge(bl)-[:bigg__hasAddressCity{selected:true}]->(lc)
Merge(bl)-[:bigg__hasAddressProvince{selected:true}]->(lp)
Merge(gb)-[:bigg__hasSpace]->(s)
Merge(s)-[:bigg__hasArea{selected:true}]->(a)
Merge(a)-[:bigg__hasAreaUnitOfMeasurement]->(atu)
Merge(a)-[:bigg__hasAreaType]->(at)
Merge(s)-[:bigg__isAssociatedWithElement]->(e)
Merge(s)-[:bigg__hasBuildingSpaceUseType{selected:true}]->(ut)
```
</details>

----

## 4. Infraestructures Organization 
 - namespace: `https://infraestructures.cat#`
 - username: `icat`

<details>
  <summary>Load infraestructures data</summary>

### 4.1. Set up organization and data sources

```bash
python3 -m set_up.Organizations -f data/Organizations/infraestructures-organizations.xls -name "Infraestructures.cat" -u "icat" -n "https://infraestructures.cat#"
```

### 4.2. Harmonize the static data
Load from HBASE (recomended when re-harmonizing)

```bash
python3 -m harmonizer -so BIS -u "icat" -n "https://infraestructures.cat#" -c
```

<details>
  <summary>Load from KAFKA (online harmonization)</summary>

1. start the harmonizer and store daemons:
```bash
python3 -m harmonizer
python3 -m store
```

2. Launch the gather utilities
```bash
python3 -m gather -so BIS -f "data/BIS/BIS-infraestructures.xls" -u "icat" -n "https://infraestructures.cat#" -st kafka
```
</details>


### 4.3. Link building with the closest Weather Station
```bash
echo "Link WS with Buildings"
python3 -m set_up.Weather -cn "ES" -f data/Weather/cpcat.json -n "https://weather.beegroup-cimne.com#" -u
```

</details>

----
## 5. Bulgaria Organization
 - namespace: `https://bulgaria.bg#`
 - username: `bulgaria`
<details>
    <summary>Load bulgaria data</summary>

### 5.1. Set up organization and data sources

```bash
echo "main org"
python3 -m set_up.Organizations -f data/Organizations/bulgaria-organizations.xls -name "Bulgaria" -u "bulgaria" -n "https://bulgaria.bg#"
echo "summary source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d SummarySource
echo "simpleTariff source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d SimpleTariffSource
echo "co2Emisions source"
python3 -m set_up.DataSources -u "bulgaria" -n "https://bulgaria.bg#" -f data/DataSources/bulgaria.xls -d CO2EmissionsSource
```

### 5.2. Harmonize the static data
Load from HBASE (recomended when re-harmonizing)
```bash
python3 -m harmonizer -so Bulgaria -u "bulgaria" -n "https://bulgaria.bg#" -c
```
<details>
    <summary>Load from KAFKA (online harmonization)</summary>

1. start the harmonizer and store daemons:
```bash
python3 -m harmonizer
python3 -m store
```
2. Launch the gather utilities

```bash
python3 -m gather -so Bulgaria -f "data/Bulgaria" -u "bulgaria" -n "https://bulgaria.bg#" -st kafka
```
</details>


### 5.3. Link building with the closest Weather Station

```bash
echo "Link WS with Buildings"
python3 -m set_up.Weather -cn "BG" -f data/Weather/locbg.json -n "https://weather.beegroup-cimne.com#" -u
```

### 5.4. Create a new Tariff and co2Emissions for the organization
The creation queries are made custom or manually or by the UI
```cypher
Match (o:bigg__Organization{userID:"bulgaria"})
Match (s:SimpleTariffSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__Tariff:Resource{bigg__tariffCompany:"CIMNE", bigg__tariffName: "electricdefault", uri: "https://bulgaria.bg#TARIFF-SimpleTariffSource-bulgaria-electricdefault"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"bulgaria"})
Match (s:SimpleTariffSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__Tariff:Resource{bigg__tariffCompany:"CIMNE", bigg__tariffName: "gasdefault", uri: "https://bulgaria.bg#TARIFF-SimpleTariffSource-bulgaria-gasdefault"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"bulgaria"})
Match (s:CO2EmissionsSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__CO2EmissionsFactor:Resource{bigg__CO2EmissionsStation:"bulgariaElectric", wgs__lon:24.422, wgs__lat:42.721, uri: "https://bulgaria.bg#CO2EMISIONS-bulgariaElectric"})-[:importedFromSource]->(s)
return t;

Match (o:bigg__Organization{userID:"bulgaria"})
Match (s:CO2EmissionsSource) where (s)<-[:hasSource]-(o)
Merge (t:bigg__CO2EmissionsFactor:Resource{bigg__CO2EmissionsStation:"bulgariaGas", wgs__lon:24.422, wgs__lat:42.721, uri: "https://bulgaria.bg#CO2EMISIONS-bulgariaGas"})-[:importedFromSource]->(s)
return t;
```
### 5.5. Link all buildings to tariff and CO2Emissions
```
Match (bigg__Organization{userID:"bulgaria"})-[:hasSource]->(:SimpleTariffSource)<-[:importedFromSource]-(t:bigg__Tariff{bigg__tariffName:"electricdefault"})
Match (dt {uri:"http://bigg-project.eu/ontology#Electricity"})
Match (bigg__Organization{userID:"bulgaria"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (c:bigg__ContractedTariff:Resource{bigg__contractStartDate: datetime("2000-01-01T00:00:00.000+0100"), bigg__contractName:"electricdefault", uri: s.uri+"_tariff"})
Merge (s)-[:bigg__hasContractedTariff]->(c)
Merge (c)-[:bigg__hasTariff]->(t)
return t;

Match (bigg__Organization{userID:"bulgaria"})-[:hasSource]->(:SimpleTariffSource)<-[:importedFromSource]-(t:bigg__Tariff{bigg__tariffName:"gasdefault"})
Match (dt {uri:"http://bigg-project.eu/ontology#Gas"})
Match (bigg__Organization{userID:"bulgaria"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (c:bigg__ContractedTariff:Resource{bigg__contractStartDate: datetime("2000-01-01T00:00:00.000+0100"), bigg__contractName:"gasdefault", uri: s.uri+"_tariff"})
Merge (s)-[:bigg__hasContractedTariff]->(c)
Merge (c)-[:bigg__hasTariff]->(t)
return t;

Match (bigg__Organization{userID:"bulgaria"})-[:hasSource]->(:CO2EmissionsSource)<-[:importedFromSource]-(co2:bigg__CO2EmissionsFactor{bigg__CO2EmissionsStation:"bulgariaElectric"})
Match (dt {uri:"http://bigg-project.eu/ontology#Electricity"})
Match (bigg__Organization{userID:"bulgaria"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (s)-[:bigg__hasCO2EmissionsFactor]->(co2)
return co2;

Match (bigg__Organization{userID:"bulgaria"})-[:hasSource]->(:CO2EmissionsSource)<-[:importedFromSource]-(co2:bigg__CO2EmissionsFactor{bigg__CO2EmissionsStation:"bulgariaGas"})
Match (dt {uri:"http://bigg-project.eu/ontology#Gas"})
Match (bigg__Organization{userID:"bulgaria"})-[:bigg__hasSubOrganization*]->()-[:bigg__managesBuilding]->()-[:bigg__hasSpace]->()-[:bigg__hasUtilityPointOfDelivery]->(s)-[:bigg__hasUtilityType]->(dt)
Merge (s)-[:bigg__hasCO2EmissionsFactor]->(co2)
return co2;
```

### 5.6. Load tariff and co2 timeseries
```bash
python3 -m harmonizer -so SimpleTariff -u bulgaria -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGridElectricity" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/Euro" -n "https://bulgaria.bg#" -c 
python3 -m harmonizer -so SimpleTariff -u bulgaria -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGas" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/Euro" -n "https://bulgaria.bg#" -c 
python3 -m harmonizer -so CO2Emissions -u bulgaria -mp "http://bigg-project.eu/ontology#CO2Emissions" -p "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -pu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/KiloGM" -n "https://bulgaria.bg#" -c 
python3 -m harmonizer -so CO2Emissions -u bulgaria -mp "http://bigg-project.eu/ontology#CO2Emissions" -p "http://bigg-project.eu/ontology#EnergyConsumptionGas" -pu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/KiloGM" -n "https://bulgaria.bg#" -c 
```
<details>
<summary>Load from kafka</summary>

```bash
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_ELECSP_test01.xlsx -u bulgaria -di 2015-01-01 -de 2030-01-01 --co2_uid bulgariaElectric -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/KiloGM" -n "https://bulgaria.bg#" -st kafka 
python3 -m gather -so CO2Emissions -f data/CO2Emissions/EMISSIONS_FACT_GASNAT_test01.xlsx -u bulgaria -di 2015-01-01 -de 2030-01-01 --co2_uid bulgariaGas -mp "http://bigg-project.eu/ontology#CO2Emissions" -cp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -cpu "http://qudt.org/vocab/unit/KiloW-HR" -unit "http://qudt.org/vocab/unit/KiloGM" -n "https://bulgaria.bg#" -st kafka
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_ELEC_test01.xlsx -u bulgaria -di 2015-01-01 -de 2030-01-01 -tar electricdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGridElectricity" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGridElectricity" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://bulgaria.cat#" -st kafka
python3 -m gather -so SimpleTariff -f data/Tariff/Tariff_GASNAT_test01.xlsx -u bulgaria -di 2015-01-01 -de 2030-01-01 -tar gasdefault -mp "http://bigg-project.eu/ontology#Price.EnergyPriceGas" -pp "http://bigg-project.eu/ontology#EnergyConsumptionGas" -ppu "http://qudt.org/vocab/unit/KiloW-HR" -cu "http://qudt.org/vocab/unit/Euro" -n "https://bulgaria.cat#" -st kafka 

```
</details>


### 5.7. Create Device AGGREGATORS
```bash
echo "DeviceAggregators datadis"
python3 -m set_up.DeviceAggregator -cn "BG" -t "totalElectricityConsumption" -cn BG
echo "DeviceAggregators nedgia"
python3 -m set_up.DeviceAggregator -cn "BG" -t "totalGasConsumption" -cn BG
echo "DeviceAggregators weather"
python3 -m set_up.DeviceAggregator -cn "BG" -t "externalWeather" -cn BG
```

</details>


----
## 6. Load UI requirements

### 6.1 Create Languages
```cypher
Merge(a:Language{iso__code: 'es'}) SET a.name='Castellano', a.labels=['Castellano@es', 'Spanish@en', 'Castellà@ca', 'испански@bg'];
Merge(a:Language{iso__code: 'ca'}) SET a.name='Catalán', a.labels=['Catalán@es', 'Catalan@en', 'Català@ca', 'каталонски@bg'];
Merge(a:Language{iso__code: 'en'}) SET a.name='Inglés', a.labels=['Inglés@es', 'English@en', 'Anglès@ca', 'Английски@bg'];
Merge(a:Language{iso__code: 'bg'}) SET a.name='Búlgaro', a.labels=['Búlgaro@es', 'Bulgarian@en', 'Búlgar@ca', 'български@bg'];
```

### 6.2 Create Roles
```cypher
Merge (a:Authority {name: 'ROLE_SUPERUSER'});
Merge (a:Authority {name: 'ROLE_ORGANIZATION_ADMINISTRATOR'});
Merge (a:Authority {name: 'ROLE_BUILDING_ADMINISTRATOR'});
Merge (a:Authority {name: 'ROLE_BUILDING_USER'});
```

### 6.3 Update organizations
```cypher
// Bulgaria
MATCH (l1:Language{iso__code: 'bg'}) 
MATCH (l2:Language{iso__code: 'en'}) 
MATCH (o1:bigg__Organization{userID:'bulgaria'}) 
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l1)
Merge (o1)-[:hasAvailableLanguage {selected: true, languageByDefault: true}]->(l2); 

// Generalitat
MATCH (l1:Language{iso__code: 'ca'}) 
MATCH (l2:Language{iso__code: 'es'}) 
MATCH (l3:Language{iso__code: 'en'}) 
MATCH (o1:bigg__Organization{userID:'icaen'}) 
Merge (o1)-[:hasAvailableLanguage {selected: true, languageByDefault: true}]->(l1)
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l2)
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l3)

// Infraestructures
MATCH (l1:Language{iso__code: 'ca'}) 
MATCH (l2:Language{iso__code: 'es'}) 
MATCH (l3:Language{iso__code: 'en'}) 
MATCH (o1:bigg__Organization{userID:'icat'}) 
Merge (o1)-[:hasAvailableLanguage {selected: true, languageByDefault: true}]->(l1)
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l2)
Merge (o1)-[:hasAvailableLanguage {selected: false, languageByDefault: false}]->(l3)
```

### 6.4 Update organization settings
```cypher
Match(n:bigg__Organization{userID:"bulgaria"}) 
Merge (n)-[:hasConfig]->(c:AppConfiguration{createBuilding: true, epc:"bulgaria", uploadManualData: true});
Match(n:bigg__Organization{userID:"icaen"}) 
Merge (n)-[:hasConfig]->(c:AppConfiguration{createBuilding: false, epc:"CEEX3X", uploadManualData: false});
Match(n:bigg__Organization{userID:"icat"}) 
Merge (n)-[:hasConfig]->(c:AppConfiguration{createBuilding: false, epc:"CEEX3X", uploadManualData: false});
```

### 6.5 Create superuser 
```cypher
//CREATE USER
Merge (p:bigg__Person:Resource {uri:"https://cimne.beegroup.com/users#admin"})
SET p.bigg__name='Admin', p.bigg__userName='admin', p.blocked= false, 
    p.createDate= datetime('2022-07-15T10:00:00.000000000Z'), p.email= 'admin@fake', 
    p.lastModifiedDate= datetime('2022-07-15T10:00:00.000000000Z'), p.lastName= 'admin', 
    p.password= 'encripted_password', p.validated= true;
//ROLE
MATCH (p:bigg__Person{bigg__userName:'admin'})
MATCH (a:Authority{name:'ROLE_SUPERUSER'})
MERGE (p)-[:hasRole]->(a);

// LINK ORGANIZATIONS
MATCH (p:bigg__Person{bigg__userName:'admin'})
MATCH (o:bigg__Organization) WHERE not o.userID is null
MERGE (p)-[:bigg__managesOrganization]->(o);

// hasDefaultLanguage
MATCH (p:bigg__Person{bigg__userName:'admin'})
MATCH (l:Language{name:'Catalán'}) 
CREATE (p)-[r:hasDefaultLanguage]->(l);
```

# BUILD DOCKER
   docker buildx build --platform linux/amd64,linux/arm64 --push -t docker.tech.beegroup-cimne.com/jobs/importing_tool . --provenance false
