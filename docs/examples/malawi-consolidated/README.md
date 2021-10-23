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
4. Clone or copy or create a symlink to the pih-pentaho repository into the [jobs](./jobs) folder
5. Build this project and copy the jar file from the target directory into this PETL home directory as petl.jar

The result should be a folder structure like this:

```bash
~/environments/malawi-consolidated-example/
  ├── jobs/
      ├── refresh-consolidated-warehouse.yml
      ├── refresh-upper-neno.yml
      ├── refresh-lower-neno.yml
      ├── pih-pentaho/
          ├── jobs/...
          ├── ...
          ├── malawi
              ├── jobs...
              ├── schema...
              ├── transforms...
  ├── application.yml
  ├── petl.jar
  
```

### Prepare source OpenMRS MySQL databases to use

This example assumes that the user is able to set-up two separate MySQL databases, one with data for Upper Neno, 
and the other with data for Lower Neno.  Update the "upperNenoOpenmrs" and "lowerNenoOpenmrs" properties within
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
up with Docker if you don't otherwise have one available.  You will be able to connect to this using your SQL client 
of choice (Intellij, Toad, etc).  You can also choose to use an existing SQL Server instance (eg. in Azure or otherwise) 
if you have one available.  

Update the "consolidatedReporting" property within the [application.yml](./application.yml) file with the relevant 
server, name, port, user, and password for this database.

### Execute the job

From your PETL home directory, execute:
```java -jar petl.jar```

## Features highlighted in this example

### Startup Jobs

Startup jobs provide a mechanism to execute a specific set of jobs at the time of PETL startup and prior to any other
jobs that are auto-detected and executed in the jobs directory.  This provides two main benefits:

#### Initialization

Any initialization setup can be done here that is shared by multiple jobs.  In this case, we are executing 
[initialize.yml](./jobs/initialize.yml), which is a SQL execution job that executes a series of SQL scripts against the
target database.  This allows us to do things like [create shared functions](./jobs/function-num-columns-changed.sql) and 
[procedures](./jobs/function-drop-table-if-exists.sql), or setup database-wide objects like 
[partition functions and schemes](./jobs/initialize-partitions.sql).

#### Testing

If we are testing a particular job or testing changes to the PETL codebase, it may be easiest to add the job that you are 
testing with to the startupJobs, as this bypasses the scheduling of the job and executes immediately at startup

### Loading from multiple sources into a single table using partitioning

The overall process is this:

Use a [job-pipeline](./jobs/load-encounters.yml) to execute the following in series:

**Job 1:**

Use a **SQL Execution Job** to:

* [Create a table](./jobs/encounters_schema.sql) to contain the unified data using this partition function. 
* [Recreate this table any time the schema changes](./jobs/encounters_recreate_schema_if_needed.sql).

**Job 2:**

Use an **Iterating Job**

* Iterate across all of our data sources, and execute each (up to 10) concurrently, with up to 5 attempts per iteration

* [Drop and create a table](./jobs/encounters_drop_and_create.sql) to use just to load data for this iteration
* [Extract data from the datasource](./jobs/encounters_extract_query.sql) into this table (we simulate using different datasources by having different table rows)
* [Moving data into the unified table](./jobs/encounters_move_partition.sql) by moving the partition over and dropping the iteration-specific table.
