PETL - Partitioning Example
==========================

This example aims to demonstrate how one can use a variety of PETL jobs together to efficiently load a unified
target reporting database from multiple source databases.  This example demonstrates many of PETL's capabilities,
in terms of the breadth of job types used, execution modes, and more sophisticated techniques.

This example simulates the effect of connecting to different database instances by instead connecting to a single
database instance but reading a subset of data each time, and merging these into a target table.

## How to run this example

### Install and configure PETL to a test environment

1. Create a PETL Home directory, and copy the included [datasources](./datasources) and [jobs](./jobs) directories here.
2. Copy the provided [application.yml](./application.yml) file into this PETL home directory
3. Build this project and copy the jar file from the target directory into this PETL home directory

### Prepare a source OpenMRS MySQL database to use

The query in this example uses data from the encounter table, specifically one with a variety of encounters at 3 locations 
with the following names:  'Sal Gason', 'Sal Fanm', 'Sal Izolman'.  Using a backup of, or connecting directly to, HUM-CI 
provides a good source environment with a decent and variable amount of purely test data. Once you have this database 
available, update the configuration settings to match in [application.yml](./application.yml)

### Prepare a target SqlServer database to use.

The [Docker Compose](./docker-compose.yml) file included will enable you to get a SQL Server instance up if you don't 
otherwise have one available.  Simply edit this file to specify the SA password that you wish to use, and run "docker-compose up -d" 
from the directory containin this file.  You will be able to connect to this using your SQL client of choice (Intellij, Toad, etc).  
Update the configuration settings to match in [application.yml](./application.yml)

Connect to this as the SA user and create the target database:
```CREATE DATABASE openmrs_reporting;```

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
