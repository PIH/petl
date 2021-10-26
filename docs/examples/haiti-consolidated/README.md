PETL - Haiti Consolidated Warehouse Example
=============================================

This example aims to demonstrate how to set up a Haiti reporting environment.
This includes executing jobs against 1-N databases and combining the loaded data in a unified set of warehouse tables in SQL Server

## How to run this example

### Install and configure PETL to a test environment

1. Create a folder in a location of your choice and make note of this.  This is your "PETL home" directory for this example.
2. Copy the provided [application.yml](./application.yml) file into this PETL home directory
3. Build the [openmrs-config-zl](https://github.com/PIH/openmrs-config-zl) project (`mvn clean install`)
4. Copy the contents of openmrs-config-zl/target/openmrs-packager-config/configuration to <PETL home>/openmrs-config-zl/configuration
5. Create a symbolic link from <PETL home>/datasources to configuration/pih/petl/jobs (the symbolic link is necessary, as some jobs reference other configuration SQL files outside of PETL)
6. Copy the contents of configuration/pih/petl/datasources to <PETL home>/datasources
7. Ensure you have the latest PETL jar.  ```mvn clean install``` and copy the jar file from the target directory into this PETL home directory as petl.jar

The result should be a folder structure like this:

```bash
~/environments/haiti-consolidated-example/
  ├── datasources/
      ├── openmrs-cange.yml
      ├── openmrs-hinche.yml
      ├── openmrs-hsn.yml
      ├── ...
      ├── warehouse.yml
  ├── jobs/
      ├── openmrs/
      ├── warehouse/
      ├── load-warehouse-tables.yml
      ├── prepare-warehouse-tables.yml
      ├── refresh-full.yml
  ├── application.yml
  ├── petl.jar
  
```

### Prepare source OpenMRS MySQL databases to use

This example assumes that the user is able to set-up source MySQL databases, one for each site to include in the warehouse. 
For each source database, one should add or update a corresponding openmrs-<site>.yml file in the datasources directory,
and should ensure that the appropriate server, name, port, user, and password for these databases is set up to populate these in [application.yml](./application.yml).

### Prepare a target Sql Server database to use.

See the documention under [SQL Server Docker](../sqlserver-docker) for guidance on how to set up a SQL Server instance 
with Docker if you don't otherwise have one available.  You will be able to connect to this using your SQL client 
of choice (Intellij, Toad, etc).  You can also choose to use an existing SQL Server instance (eg. in Azure or otherwise) 
if you have one available.  If you use the provided SQL Server image, it will create a database for you when you run it.
If you use an existing SQL Server instance, you need to ensure you have an empty database created to use as the target.

Update the "datasources.warehouse" property within the [application.yml](./application.yml) file with the relevant 
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
