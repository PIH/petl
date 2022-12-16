# openmrs-mysql

In order for the Debezium-based CDC to work in any of these examples, we need a source database that has the mysql
binlog enabled.  This requires using either a MySQL 5.6 or 5.7 instance with row-level bin logging explicitly enabled 
in the MySQL configuration, or it requires MySQL 8.x where row-level bin logging has been enabled by default.

To facilitate testing, included is [a script](./upgrade-to-mysql-8.sh) that takes a mysqldump that was generated from a MySQL 5.6 instance,
and uses Docker to upgrade this to MySQL 8, which has row-level bin logging enabled by default.  It results in a MySQL
data directory that subsequent docker containers can be configured to use when they start up using Docker compose.

The initial use case for this is to take a mysqldump from humci to use as the source data, in order to 
be able to test against a realistic database with fake data that already exists.  This will allow testing such aspects as
confirming that historic data change events are included as well as new changes.

## Usage:

For example, run:

```shell
./upgrade-to-mysql-8.sh \
    --inputSqlFile=~/humci-export.sql \
    --targetMysqlDataDir=~/mysql/data
```
