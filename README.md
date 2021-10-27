PETL - PIH ETL Framework
==========================

PETL is a lightweight Spring-boot application that enables execution of a configured set of ETL jobs.
The initial objectives of PETL have been to satisfy the following objectives:

* Provide a simple tool that can execution ETL transformations that are defined in external configuration files
* Provide an easy to understand and author syntax for authoring ETL jobs
* Enable support for Pentaho/Kettle, and do so without requiring the full PDI installation
* Enable support for extracting source data out of one database and into a new schema in another database, specifically supporting MySQL -> Sql Server

# Installation

## Install Java

The PETL application is delivered as an executable JAR file.  This means that PETL requires only the Java Runtime
installed in order to run it.  The recommended Java version is **OpenJDK 8 JDK**.

## Install PETL

Typically this process would be automated using Puppet, Ansible, or a similar tool.  The Puppet module for this for the 
PIH EMR can be found here:  https://github.com/PIH/mirebalais-puppet/tree/master/mirebalais-modules/petl

The recommended steps for installing PETL are the following:

* Create a dedicated "petl" user and group, and a home directory at `/home/petl` with the following structure:

```bash
-- home
   |-- petl
       |-- bin
       |-- data
       |-- work
```

#### Install the PETL Binary

Add the PETL jar file into `/home/petl/bin`

* The latest release of PETL can be downloaded from here:  http://bamboo.pih-emr.org/artifacts/
* You can also build the petl jar from source.  See developer section below.
* You should either rename this file to `petl.jar` or create a symbolic link to this file from `petl.jar`

#### Configure the PETL application

Add a PETL configuration file named `application.yml` into `/home/petl/bin`.  This file will extend and
override the default configuration values found in [application.yml](src/main/resources/application.yml).  This
file supports the following configuration settings:

* **Logging Levels**:  Allows you to configure the logging level for the application
  
```yaml
logging:
  level:
    root: "WARN"
    org.pih: "INFO"
```

* **Job Execution Persistence**:  Allows you to configure where job execution history information is persisted 
    and how Spring, Hibernate, and the Quartz scheduler are configured.
  
```yaml
spring:
  datasource:
    platform: "h2"
    driver-class-name: "org.h2.Driver"
    url: "jdbc:h2:file:${petl.homeDir}/data/petl;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE"
    username: "sa"
    password: "Test123"
    data: "classpath:schema-h2.sql"
  jpa:
    hibernate:
      ddl-auto: "none"
  quartz:
    job-store-type: "memory"
```

* **PETL Job and Datasource Locations**:  This allows specification of where job and datasource configuration files
  are located for all of the ETL jobs.  By default, these are located in
  `/home/petl/config/datasources` and `/home/petl/config/jobs` but these can be overridden as needed here:
  
```yaml
petl:
  homeDir: "/home/petl"
  datasourceDir: "${petl.homeDir}/config/datasources"
  jobDir: "${petl.homeDir}/config/jobs"
```

* **PETL Schedule**:  Jobs can be scheduled individually (see "Overview of Jobs" below) but a global schedule can also be provided.  All jobs without an individual schedule defined in their job.yml will be run according to this schedule.  See the "Overview of Jobs" section below for more details about the supported Cron format.

```yaml
petl:
  ...
  schedule:
    cron: "0 0 5 ? * *"     # Cron-like expression that determines the execution frequency (see below)
```

* **PETL Startup Jobs**:  Jobs can be configured to run at startup in a deterministic order prior to any other scheduled jobs.

```yaml
petl:
  ...
  startup:
    jobs:
      - "job1.yml"
```


* Other server configuration:

```yaml
server:
  port: 8080
```
  
* **Any other arbitrary configuration values that you wish to refer to within your Job and Datasource files**
  As evidenced above, you can have a config value like `petl.homeDir` and then use that config value 
  within other config values.  For example, PIH typically adds variables here containing database connection information
  that can then be referenced by other configuration files.  
  See [Puppet installation code here](https://github.com/PIH/mirebalais-puppet/blob/master/mirebalais-modules/petl/templates/application.yml.erb).

#### Install the PETL service

The recommended way to run PETL is as a service that is always running.

Spring Boot facilitates setting this up by enabling the petl.jar file to be linked directly as a service in `/etc/init.d`.
See:  https://github.com/PIH/mirebalais-puppet/blob/master/mirebalais-modules/petl/manifests/init.pp

In order to configure this appropriately, one should also install a file named `petl.conf` into `/home/petl/bin`.
This configuration file should contain environment variables that affect the petl service execution.  For example:

```bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
JAVA_OPTS=-Xmx2048m -Xms1024m
```

# Configuration

## Overview of Data Sources

PETL is designed such that jobs can connect to data sources to use to extract and load data.  In order to allow 
configuring and referencing this datasources, PETL supports yaml-based configuration files.

As described in the installation section, PETL is configured such that all data source configuration files are found in 
the directory defined by `${petl.datasourceDir}`, which defaults to `/home/petl/config/datasources`

Within this directory, one datasource is defined by one yml file with the following structure:

```yaml
databaseType: "mysql"
host: "localhost"
port: "3308"
databaseName: "openmrs"
user: "openmrs"
password: "openmrs"
options: "autoReconnect=true&sessionVariables=default_storage_engine%3DInnoDB&useUnicode=true&characterEncoding=UTF-8&serverTimezone=US/Eastern"
```

Typically, once the initial setup for a given ETL server is done, the datasources involved rarely change.  Jobs may evolve, but the
datasources they connect to are more static.

For the PIH EMR, datasources are typically setup to refer to configuration variables that are set by puppet into `application.yml`.
See:  https://github.com/PIH/openmrs-config-pihemr/tree/master/configuration/pih/petl/datasources

## Overview of Jobs

PETL is designed to execute ETL jobs on a scheduled basis.  It is designed to be flexible, to support different ETL
job types, to support a fully configurable job directory hierarchy and organization, and to allow each job to define
the scheduled frequency that it should execute.

When PETL is running, the service will constantly search for any jobs that are configured within the job directory.
This directory, as described in the installation section, is configured in the `application.yml` file, and defaults to
`/home/petl/config/jobs`

Any YAML file that is present in the jobs directory or subdirectory that contains the appropriate job file structure 
will be interpreted as a job file and PETL will parse it to determine whether or not it should execute.  This allows
implementations to maintain their job files in whatever organizational hierarchy makes sense and to name these files 
however they want.  The structure of a `job.yml` file looks like the following:

```yaml
type: 'job-type'            # valid types are sqlserver-bulk-import, pentaho-job, job-pipeline
schedule:
    cron: "0 0 5 ? * *"     # Cron-like expression that determines the execution frequency (see below)
configuration:              # Each job type supports different properties within configuration
```

Each `job.yml` file indicates:

* What type of job it is.  This determines how it is configured and executed.
* How often should it run.  This uses a [cron-like expression](https://www.quartz-scheduler.org/api/2.1.7/org/quartz/CronExpression.html).  
  Note that the cron format includes a "seconds" component, so to run at 6:30 AM would be `"0 30 6 ? * *"`, not `"30 6 * ? * *"`
* How it is configured.  Each type of job will have a different set of supported configuration settings.

## Supported Job Types

Currently PETL supports 3 types of jobs:

### sqlserver-bulk-import

This type of job supports a single extraction query from MySQL which streams into a newly dropped and created SqlServer table.

One of the primary initial use cases of PETL was to facilitate extracting data from an OpenMRS MySQL database and loading it into 
a SqlServer database, either on-prem or in the cloud, to enable DirectQuery to use it as a data source.  Below is a superset
of the available configuration properties, along with a sample value and a descriptive comment:

```yaml
type: "sqlserver-bulk-import"
configuration:  
  extract:
    datasource: "mysql/openmrs.yml"   # This is the datasource that we query to retrieve the data to load
    conditional: "select if(count(*)>0,true,false) from information_schema.tables where table_name = 'hivmigration_data_warnings'"  # An optional sql statement, executed against the extract datasource. If this returns false, the job is not executed.
    context: "extract/context.sql"    # This is an additional set of sql statements added to the source execution.  Often used to set things like locale.
    query:  "covid19/admission/hivmigration_data_warnings_extraction.sql"  # This is the actual extract statement
    extraColumns:
      column_1: "'static text'"  # This would add, to the extract query, and additional select column named 'column_1' with 'static text' as the value for all rows
      column_2: "8"  # This would add, to the extract query, and additional select column named "column_2" with static number 8 for all rows
      column_3: "now()" # This would add, to the extract query, and additional select column named "column_3" with the value from the date function for all rows

  load:
    datasource: "sqlserver/openmrs_extractions.yml"  # This is the datasource that we load the extracted data into
    table: "covid_admission"  # This is the table that is created to load the data into.  It is dropped and recreated each execution unless the "dropAndRecreateTable" is set to false
    schema: "covid19/admission/hivmigration_data_warnings_schema.sql"  # This is the create table statement that is executed to create the target table.  This is optional.  If null, it is assumed the table exists.
    dropAndRecreateTable: "false"  # Optional, default is true, which will drop and recreate the target table (if it exists) each time this is run
```
   
NOTE:

* The "extraction" YAML file (in the above example, `${petl.jobDir}/covid19/admission/hivmigration_data_warnings_extraction.sql`) may perform multiple queries, 
  create temporary tables, etc, but the final statement should be a `select` that extracts the data out of MySQL.

* The "load" YAML file (in the above example, `target.yml`) generally is a single `create table` command used to create
  the table to load the data into.  Therefore it should match the schema of the `select` at the end of the extract sql.

* The "load.schema" attribute is optional.  If not specified, then no target table creation or deleting is done by the job.

### pentaho-job

This type of job executes a Pentaho Kettle Job (.kjb) file. The minimum required configuration property is the path to this file.

One can optionally specify the logging level that Pentaho Kettle should use.  Values that are supported (from most verbose to least verbose, include):
ROWLEVEL, DEBUG, DETAILED, BASIC, MINIMAL, ERROR. The default is "MINIMAL" if not explicitly configured.

Beyond these built-in configuration options, users can configure any other arbitrary values within the configuration
section, and these will be made available to all jobs and transforms to refer to a variables.  All nested objects within
the yaml configuration will be flattened with dot syntax.  For example the Job File Path above would be available to refer
to as a variable named "job.filePath"

For example, jobs within the [pih-pentaho](https://github.com/pih/pih-pentaho) project will expect the following configuration settings:

* pih.pentahoHome:  Defines the location where the pih-pentaho source is available
* pih.country:  Defines the country to run the job for, which is used within various kjb and ktr
* source and target datasource information

This job currently runs Kettle 9.1.0.0-324.  To develop jobs and transforms within this version, you can download:
https://sourceforge.net/projects/pentaho/files/Pentaho%209.1/client-tools/pdi-ce-9.1.0.0-324.zip/download

An example configuration to run a Pentaho job with BASIC level of logging at 5am every morning:

```yaml
type: "pentaho-job"
configuration:
  job:
    filePath: "pentaho/src/jobs/refresh-warehouse.kjb"
    logLevel: "BASIC"
  pih:
    pentahoHome: "${petl.jobDir}/pentaho/src"
    country: "haiti"
  openmrs:
    db:
        host: "localhost"
        port: "3306"
        name: "openmrs"
        user: "root"
        password: "rootpw"
  warehouse:
    db:
        host: "localhost"
        port: "3306"
        name: "openmrs_warehouse"
        user: "root"
        password: "rootpw"
        key_prefix: "10"
```

# job-pipeline

A pipeline job allows combining multiple jobs together into a single job.  The execution of these jobs is configurable by an optional
"execution" element.  If not specified, the default execution settings are:

```yaml
execution:
  maxConcurrentJobs: 1
  maxRetriesPerJob: 0
  retryInterval: 5
  retryIntervalUnit: "MINUTES"
```

Jobs can either be specified inline or by referencing a separate job.yml file as shown below.

Example configuration:

```yaml
type: "job-pipeline"
configuration:
  jobs:
    - path: "load-upper-neno.yml"
    - type: "job-pipeline"
      configuration:
        jobs:
          - "load-lower-neno.yml"
  execution:
    maxConcurrentJobs: 1
    maxRetriesPerJob: 5
    retryInterval: 30
    retryIntervalUnit: "MINUTES"
schedule:
    cron: "0 0 5 ? * *"  
```

# iterating-job

An iterating job allows a single job template to be executed for multiple iterations, in which each iteration can specify
different variables that can configure the job template.  Any aspect of the yaml configuration, or any nested configuration files
can refer to a variable as ${variableName}, and this will be replaced by the specified value for each iteration.

Like the job-pipeline job, an iterating-job also allows specifying an "execution" element to control whether each iteration
is run in series or in parallel, the max number of concurrent executions to run, and whether and how to retry iterations
on failure.  See details in the "job-pipeline" job above for specifying the execution within the job configuration.

Also like the job-pipeline, jobs can either be specified inline or by referencing a separate job.yml file.

For an example configuration, please see the [partitioning example here](./docs/examples/partitioning/jobs/load-encounters.yml)

# sql-execution

A SQL Execution job is a job that simply allows for one or more scripts to be executed against the configured datasource.
This is useful particularly to control exactly how and when target tables and other database objects (functions, procedures, partition schemes, etc)
are created.  Please see below for a summary of all available configuration options:

Example configuration:

```yaml
    - type: "sql-execution"
      configuration:
        datasource: "openmrs_reporting_sqlserver.yml"  # This is the datasource that the SQL should be executed against
        variables:
          locale: "en"     # Any key/value pairs defined here can be used as string replacement variables in any of the scripts below, by referring to them like ${locale}
        delimiter: "#"     # If specified, this will split each of the listed scripts up into multiple statements using the given delimiter, and execute each statement sequentially
        scripts:
          - "encounters_schema.sql"  # The first script to execute
          - "encounters_recreate_schema_if_needed.sql"  # The second script to execute.  Any number of scripts can be listed
```

# create-table

A create-table job simply facilitates creating a table in a given datasource.  It is particularly useful as a means to create one table based on the schema
from another table.  See below for available options:

Example configuration:

```yaml
    - type: "create-table"
      configuration:
        createFromTable:  # The createFromTable option allows one to indicate that the target table should replicate an existing table schema
          datasource: "openmrs.yml"  # This is the datasource that should be analyzed to get the schema to create
          tableName: "encounters"  # This is the table that should be analyzed to get the schema to create
        target:
          datasource: "reporting.yml"  # This is the datasource in which the target table should be created
          dropAndRecreateTable: "true"  # Optional, defaults to false.  If the target table already exists, and this is true, it will be dropped and recreated.  Otherwise, table is left unchanged.
 
# Developer Reference

## Building

The PETL project can be built and tested using the standard Maven commands (`mvn clean test`, `mvn clean install`).

Note that when running tests, PETL uses the testcontainers package (see https://www.testcontainers.org/) to
fire up dockerized containers of MySQL and SQL Server.  This happens via including the testcontainers Maven package
in the pom and by then using the "tc" prefix when setting up the connection information in the datasource
profiles.  (See https://github.com/PIH/petl/tree/master/src/test/resources/configuration/datasources)

## Developing and testing MySQL to SQL Server jobs

Prerequisites:

* [openmrs-config-pihemr](https://github.com/PIH/openmrs-config-pihemr) checked out (this is where the existing jobs live)
* Local MySQL instance with an OpenMRS DB

### Set up a SQL Server instance

Set up a local SQL Server instance using Docker:

* SQL Server 2019: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-ver15&pivots=cs1-bash
* SQL Server 2017: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-2017&pivots=cs1-bash

Note your password must be be at least 8 characters long and contain characters from three of the following four sets:
uppercase letters, Lowercase letters, Base 10 digits, and Symbols,  or the docker container will fail silently. 

You can run `docker logs [docker_id]` to debug

Also note that the "root" user is named "sa", so you should set the username to "sa" when attempting to connect.

Once the container is started, get a SQL shell in it and run `create database <your_db_name>`

### Create an `application.yml` file

The `application.yml` file tells PETL

* The directory to use as a working directory (`homeDir`)
* The location of the datasource and job configurations (in the example below, they point to the appropriate
directories in my local `openmrs-config-pihemr`)
* The connection credentials for the local MySQL and SQL Server databases

As an example:

```yaml
petl:
  homeDir: "/home/mgoodrich/petl"
  datasourceDir: "/home/mgoodrich/openmrs/modules/config-pihemr/configuration/pih/petl/datasources"
  jobDir: "/home/mgoodrich/openmrs/modules/config-pihemr/configuration/pih/petl/jobs"

mysql:
  host: "localhost"
  port: "3306"
  databaseName: "openmrs_mirebalais"
  user: "root"
  password: "***"
  options: "autoReconnect=true&sessionVariables=default_storage_engine%3DInnoDB&useUnicode=true&characterEncoding=UTF-8&serverTimezone=US/Eastern"

sqlserver:
  host: "localhost"
  port: "1433"
  databaseName: "openmrs_test"
  user: "sa"
  password: "******"

server:
  port: 9109
```

From the directory where you've created your `application.yml` file, run PETL via the following command.
(Note that the path to `petl-x.y.z-SNAPSHOT.jar` should be relative to the current directory you are in).

```bash
 java -jar target/petl-x.y.z-SNAPSHOT.jar
 ```

# Releasing

On each build, our Bamboo CI server deploys the Maven artifact to the 'artifacts' directory on our Bamboo server:

http://bamboo.pih-emr.org/artifacts/

So doing a "release" should be as simple as:

* Changing the pom.xml to remove the snapshot from the version number (ie, to do the "2.1.0" release, change the
  version in the pom from "2.1.0-SNAPSHOT" to "2.1.0")
  
* Commit the change and let the Bamboo job run.  Afterwards the new release (ie petl-2.1.0.jar) should appear in the artifacts directory of bamboo: http://bamboo.pih-emr.org/artifacts/

* Once this is confirmed, change the version number to the next snapshot (ie change "2.1.0" to "2.2.0-SNAPSHOT") so 
  that subsequent commits won't overwrite the "2.1.0" jar
  
* Update the "petl_version" variable in Puppet to deploy the version of Petl you want to release on each server

# TODO:

* Deploy petl to maven (snapshots and releases)
* Change from pulling petl from bamboo to maven and remove the Bamboo release steps above
* Add ability to load (initial + updates) to jobs and datasources from external location (eg. url)
* Add ability to check for updates to existing jobs and datasources from external location
* Provide web services and/or a web application that enables a broad range of users to:
  * Check for updates to current configuration based on source + version
  * Load updates to current configuration if available
  * View the details of the various jobs and the history of their execution
  * View the last update date and current status of the Pipeline(s)
  * Kick off an update of the pipeline(s)
  * View any errors in running the pipeline(s) 

# Related Projects

* PIH Puppet: https://github.com/PIH/mirebalais-puppet
* PIH-PENTAHO:  https://github.com/PIH/pih-pentaho
* PETL Ansible Scripts:  BitBucket PETL playbook/role
