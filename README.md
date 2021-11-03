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

* **PETL Startup Jobs**:  Jobs can be configured to run at startup in a deterministic order prior to any other scheduled jobs.

```yaml
petl:
  ...
  startup:
    jobs:
      - "job1.yml"
```

* **Other server configuration**:

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
will be interpreted as a job file and PETL will parse it to determine whether it should execute.  This allows
implementations to maintain their job files in whatever organizational hierarchy makes sense and to name these files 
however they want.  The structure of a `job.yml` file looks like the following:

```yaml
type: 'job-type'            # valid types are sqlserver-bulk-import, pentaho-job, job-pipeline
configuration:              # Each job type supports different properties within configuration
path: "some-template.yml"   # As an alternative to specifying type and configuration, one can refer to another job definition by path
schedule:
    cron: "0 0 5 ? * *"     # Cron-like expression that determines the execution frequency (see below)
parameters:                 # Each job can be configured with a set of parameters to assist with templating (more below)
```

Each `job.yml` file indicates:

* What type of job it is.  This determines how it is configured and executed.
* How often should it run.  This uses a [cron-like expression](https://www.quartz-scheduler.org/api/2.1.7/org/quartz/CronExpression.html).  
  Note that the cron format includes a "seconds" component, so to run at 6:30 AM would be `"0 30 6 ? * *"`, not `"30 6 * ? * *"`
* How it is configured.  Each type of job will have a different set of supported configuration settings.

### Job Templates and Variables

Job definitions can get complex, particularly if setting up pipelines and iterations that need to perform multiple coordinated steps,
set up partitioning schemes, etc.  To aid with this, all jobs support templating.  Within a particular jobs yaml definition file,
or within any files that a job may load in and use (eg. associated job or sql files), variables can be used.  These can refer to
any variable from the following sources (in order of lowest-to-highest precedence):

a) any environment variable
b) any variable defined in application.yml
c) any variable defined in the "parameters" property of a job
d) any variable defined in a specific job configuration (i.e. in the iterations property of the iterating-job)

This allows for job definitions to be created and then re-used.  For example, I could have a job file that is a reusable
job template like follows:

import-to-sqlserver.yml
```yaml
type: "sqlserver-bulk-import"
configuration:
  extract:
    datasource: "openmrs-${siteName}.yml"
    query:  "sql/extractions/${tableName}.sql"
  load:
    datasource: "warehouse.yml"
    table: "${tableName}"
    schema: "sql/schemas/${tableName}.sql"
    extraColumns:
      - name: "site"
        type: "VARCHAR(100)"
        value: "'${siteName}'"
```

I could then have jobs that execute this, either on their own or within a job-pipeline or iterating-job.  For example:

import-patient-table-to-hinche.yml
```yaml
path: "import-to-sqlserver.yml"
parameters:
  siteName: "hinche"
  tableName: "patient"
```

## Supported Job Types

Currently PETL supports the following types of jobs:

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
    query:  "covid19/admission/extract.sql"  # This is the actual extract statement

  load:
    datasource: "sqlserver/openmrs_extractions.yml"  # This is the datasource that we load the extracted data into
    table: "covid_admission"  # This is the table that is created to load the data into.  It is dropped and recreated each execution unless the "dropAndRecreateTable" is set to false
    schema: "covid19/admission/schema.sql"  # This is the create table statement that is executed to create the target table.  This is optional.  If null, it is assumed the table exists.
    extraColumns:
      - name: "column_1"        # This would add, to the extract query, and additional select column named 'column_1'
        type: "VARCHAR(100)"    # with a datatype of VARCHAR(100)
        value: "'static text'"  # with 'static text' as the value for all rows
    partition:
      scheme: "psSite"  # If specified, this will associate this partition scheme for this table when created
      column: "partition_num"  # If specified, this will use this column for the partition scheme, when created
      value: "3"  # If specified, this will use this column value for the partition, when created
    dropAndRecreateTable: "false"  # Optional, default is true, which will drop and recreate the target table (if it exists) each time this is run
```
   
NOTE:

* The "extraction" YAML file (in the above example, `${petl.jobDir}/covid19/admission/extract.sql`) may perform multiple queries, 
  create temporary tables, etc, but the final statement should be a `select` that extracts the data out of MySQL.

* The "load" YAML file (in the above example, `target.yml`) generally is a single `create table` command used to create
  the table to load the data into.  Therefore, it should match the schema of the `select` at the end of the extract sql.

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

A pipeline job allows combining multiple jobs together into a single job.  By default, this will run each defined 
job in series, and will only execute jobs if those defined before it in the list execute successfully.  One can
modify this behavior to run the listed jobs in parallel and independently of the success of prior jobs by setting 
the ```maxConcurrentJobs``` property to a value greater than 1.

```yaml
maxConcurrentJobs: 5
```

One can also control how errors are handled if encountered by nested jobs.  This is done via the ```errorHandling```
configuration element as follows:

```yaml
errorHandling:
  maxAttempts: 10
  retryInterval: 5
  retryIntervalUnit: "MINUTES" # options are NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS
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

Like the job-pipeline job, an iterating-job also defaults to executing on job at a time, in series, in the order each iteration
is listed in the configuration file, and will only execute jobs if those defined before it in the list execute successfully.  One can
modify this behavior to run the listed jobs in parallel and independently of the success of prior jobs by setting
the ```maxConcurrentJobs``` property to a value greater than 1.

```yaml
maxConcurrentJobs: 5
```

One can also control how errors are handled if encountered by nested jobs.  This is done via the ```errorHandling```
configuration element as follows:

```yaml
errorHandling:
  maxAttempts: 10
  retryInterval: 5
  retryIntervalUnit: "MINUTES" # options are NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS
```

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
        source:  # Indicates the source of the schema.  This can either be an existing table (datasource+tableName) or create table statement (sqlFile)
          datasource: "openmrs.yml"  # This is the datasource that should be analyzed to get the schema to create
          tableName: "encounters"  # This is the table that should be analyzed to get the schema to create
          sqlFile: "myschema.sql"  # You can specify a sql file with a create table statement as an alternative to the datasource and tableName
        target:
          datasource: "reporting.yml"  # This is the datasource in which the target table should be created
          tableName: "my_encounters"   # This is the table into which the schema will be created
          actionIfExists: "drop"  # Optional.  Values you can specify are "drop" and "dropIfChanged".  Default is to leave the table unchanged if it already exists.
```

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
