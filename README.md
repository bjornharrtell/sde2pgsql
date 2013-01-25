sde2pgsql
=========

Tool written i Java to export data from ArcSDE into PostgreSQL/PostGIS

Tested against ArcSDE 9.2 and PostgreSQL 9.0 with PostGIS 1.5

To compile and use you will need to place jpe92_sdk.jar and jsde92_sdk.jar from 
ArcSDE SDK in /lib. These jars are not redistributable.

The included CLI application have four mandatory parameters:

sdeName    - name of source featureclass to export
schemaName - name of source schema
buffer     - integer value to control how many features to batch process
srid       - integer value of assumed srid

Common properties (connection strings etc.) are read from file sde2pgsql.properties 

The destination will be created with same schema/table name as source.