PETL - sqlserver-docker
==========================

Many of the examples and use cases rely on a SQL Server Database to connect into and to test against
This example is here to provide an easy way to fire up a SQL Server database to use for this purpose.

Simply open a terminal in this directory, and run:

```docker-compose up -d --build```

This will create a default SQL Server instance running at localhost on port 1433.
By default this will have a root user of sa/9%4qP7b2H!%J and an initial, empty database called openmrs_reporting

You can change the password of the root user, the name of the database, and the listening port
via environment variables in the [Docker Compose](./docker-compose.yml) file.

If you don't want to use Docker compose, you can get a comparable Docker container up manually like follows:

```shell
docker run --name sqlserver -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=9%4qP7b2HJ" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "9%4qP7b2HJ" -d master -Q "CREATE DATABASE openmrs_reporting"
```