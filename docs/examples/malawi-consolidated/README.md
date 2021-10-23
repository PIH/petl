PETL - Malawi Consolidated Warehouse Example
=============================================

This example aims to demonstrate how to set up a Malawi reporting environment.
This includes:
* Executing a Pentaho Pipeline to create a MySQL warehouse for Upper Neno
* Executing a Pentaho Pipeline to create a MySQL warehouse for Lower Neno
* Executing jobs to combine these into a unified set of warehouse tables in SQL Server

## How to run this example

### Install and configure PETL to a test environment

1. Create a folder in a location of your choice and make note of this.  This is your "PETL home" directory for this example.
2. Copy the provided [application.yml](./application.yml) file into this PETL home directory
3. Copy the provided [jobs](./jobs) directory and [datasources](./datasources) directory into this PETL home directory
4. Clone or copy the [pih-pentaho](https://github.com/PIH/pih-pentaho) project into the [jobs](./jobs) folder
5. Ensure you have the latest PETL jar.  ```mvn clean install``` and copy the jar file from the target directory into this PETL home directory as petl.jar

The result should be a folder structure like this:

```bash
~/environments/malawi-consolidated-example/
  ├── jobs/
      ├── pih-pentaho/
          ├── jobs/...
          ├── ...
          ├── malawi
              ├── jobs...
              ├── schema...
              ├── transforms...
      ├── refresh-full.yml
      ├── refresh-lower-neno.yml
      ├── refresh-sqlserver.yml
      ├── refresh-upper-neno.yml
      ├── selectAllFromTable.sql
  ├── application.yml
  ├── petl.jar
  
```

### Prepare source OpenMRS MySQL databases to use

This example assumes that the user is able to set-up two separate MySQL databases, one a full OpenMRS database for Upper Neno, 
and the other a full OpenMRS database for Lower Neno.  Update the "upperNenoOpenmrs" and "lowerNenoOpenmrs" properties within
the [application.yml](./application.yml) file with the relevant server, name, port, user, and password for these databases.

### Prepare target intermediary MySQL databases to use

In the same or different MySQL instances as in the previous step, create databases into which the initial
ETL / Pentaho jobs should write data.  For example:

```bash
mysql> create database neno_reporting default charset utf8;
Query OK, 1 row affected (0.00 sec)

mysql> create database lisungwi_reporting default charset utf8;
Query OK, 1 row affected (0.00 sec)
```

Update the "upperNenoReporting" and "lowerNenoReporting" properties within
the [application.yml](./application.yml) file with the relevant server, name, port, user, and password for these databases.

### Prepare a target SqlServer database to use.

See the documention under [SQL Server Docker](../sqlserver-docker) for guidance on how to set up a SQL Server instance 
with Docker if you don't otherwise have one available.  You will be able to connect to this using your SQL client 
of choice (Intellij, Toad, etc).  You can also choose to use an existing SQL Server instance (eg. in Azure or otherwise) 
if you have one available.  If you use the provided SQL Server image, it will create a database for you when you run it.
If you use an existing SQL Server instance, you need to ensure you have an empty database created to use as the target.

Update the "consolidatedReporting" property within the [application.yml](./application.yml) file with the relevant 
server, name, port, user, and password for this database.

### Execute the job

From your PETL home directory, execute:
```java -jar petl.jar```

You should see each job execute.  The full execution time should take around 20-30 minutes, depending on server settings.
Please see comments in the various configuration files in this example for deeper explanations for what each setting means
and how they can be adjusted.

## Notes on customizing this example for actual use

* The database users and passwords provided in the example are for testing only.  Please choose new, strong passwords for production use.

* The application.yml file provided sets the refresh-full.yml job up in the "startup.jobs" property.  This is mainly there 
  to facilitate development and testing, as this is where one simply wants to fire up the application and execute their job.
  However, on a typical test or production server, you may want to remove this from the startup.jobs, as keeping it here will
  mean that anytime the server or application is restarted, the jobs will run and will drop and recreate the warehouse tables.
  You can adjust the schedule and frequency of the job execution within the "schedule" property of refresh-full.yml
  In the event you want to simply execute the job and exit the application from an external script, you would want to remove
  the schedule property from the refresh-full.yml file, and keep this job listed in the startup.jobs property of application.yml.
  This might be desirable, for example, if one needed to control the exact timing of execution in a shell script or cron job
  based on external dependencies (eg. the availability of new copies of the source database to execute against).
