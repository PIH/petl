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