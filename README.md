PETL - PIH ETL Framework
================

This is an application whose goal is to support the execution of ETL jobs for PIH
Initially, this application  was written specifically to execute Pentaho Jobs and Transforms using Kettle.
However, it has since evolved to enable the execution of other types of Jobs, as well as to schedule these jobs.

# Requirements:

* Java 8

# Related Projects:

* PIH Puppet: https://github.com/PIH/mirebalais-puppet
* PIH-PENTAHO:  https://github.com/PIH/pih-pentaho
* PETL Ansible Scripts:  BitBucket PETL playbook/role

# Objectives:

* Provide a simple tool that can execution ETL transformations that are defined in external configuration files
* Provide an easy to understand and author syntax for authoring ETL jobs
* Enable support for Pentaho/Kettle but not require it, and do so without requiring the full PDI installation

# Building and testing

The PETL project can be build and tested using the standard Maven commands (`mvn clean test`, `mvn clean install`).

Note that when running tests, PETL uses the testcontainers package (see https://www.testcontainers.org/) to
fire up dockerized containers of MySQL and SQL Server.  This happens via including the testcontainers Maven package 
in the pom and by then using the "tc" prefix when setting up the connection information in the datasource
profiles.  (See https://github.com/PIH/petl/tree/master/src/test/resources/configuration/datasources)

# Extracting from OpenMRS and loading into SQL SERVER

Although originally written to run Pentaho jobs, currently our primary use case for PETL is to extract data from an 
OpenMRS MySQL database and load it into a SQL Server DB so that the data can be more easily analyzed using PowerBI.

We install PETL on some of our OpenMRS instances via Puppet and then configure PETL to load jobs into found in the 
the "configuration/pih/petl" subdirectory of the OpenMRS data directory.  The base configuration we set up via Puppet
can be found here (look for the "petl_" parameters):

https://github.com/PIH/mirebalais-puppet/blob/master/hieradata/common.yaml#L99

Then, as an example, on HUM-CI we configure it specifically to connect to the OpenMRS instance running locally,
and then load into a SQL Server 2014 instance running at the Boston office (look for the "petl_" parameters):

https://github.com/PIH/mirebalais-puppet/blob/master/hieradata/humci.pih-emr.org.yaml#L29

## Anatomy of a OpenMRS to SQL Server PETL job

An OpenMRS-to-SQL-Server PETL job consists of:
* A SQL file, written in MySQL syntax, to extract the database out of the OpenMRS Database
* A SQL file, written in SQL Server syntax, to create the table that the extracted data should be loaded into
* A YML configuration file which defines the "extract" and "load" SQL files to use, and the schedule to run on
(Note that the cron format includes a "seconds" component, so to run at 6:30 AM would be "0 30 6 ? * *", not
"30 6 * ? * *")

As an example, see:

https://github.com/PIH/openmrs-config-pihemr/tree/master/configuration/pih/petl/jobs/vaccinations_anc  

A few things to note in the above example:

* The job.yml defines the "datasource" for "extract" and "load".  These refer to files with the specific
configuration information for the "extract" and "load" databases and can be found here:
https://github.com/PIH/openmrs-config-pihemr/tree/master/configuration/pih/petl/datasources
These files generally reference PETL configuration variables that will be set up via Puppet.  If you are adding
a new job to an existing pipeline, generally you don't need to modify these.

* The "extraction" yml file (in the above example, source.yml) may perform multiple queries, create temporary tables,
etc, but as a last set there should be a single "select" the creates the final data to extract.

* The "load" yml file (in the above example, target.yml) generally is a single "create table" command used to create
the table to load the data into.  Therefore it should match the schema of the "select" at the end of the extract sql

## Running the OpenMRS to SQL Server jobs locally

PETL can be run locally, which is helpful when developing and debugging PETL jobs.

### Prerequisites:

* "openmrs-config-pihemr" project checked out (this is where the existing jobs live): https://github.com/PIH/openmrs-config-pihemr
* Local MySQL instance with an OpenMRS DB
* Local SQL Server instance (instructions below on how to set one up via Docker)

#### Creating a Docker instance of SQL Server

Instructions can be found here:

SQL Server 2019: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-ver15&pivots=cs1-bash

SQL Server 2017: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-2017&pivots=cs1-bash

Note your password must be be at least 8 characters long and contain characters from three of the following four sets:
Uppercase letters, Lowercase letters, Base 10 digits, and Symbols,  or the docker container will fail silently. 
(You can run "docker logs [docker_id]" to debug)

Also note that the "root" user is named "sa", so you should set the username to "sa" when attempting to connect.

create database <your_db_name>

### Building PETL from source

Run `mvn clean install` to build the PETL executable from  source

### Configuration

You'll need to set up a "application.yml" file with the configuration you want to use. This tells PETL:

* the directory to use as a working directory ("homeDir")
* the location of the datasource and job configurations (in the example below, they point to the appropriate
directories in my local check-out of openmrs-config-pihemr)
* the connection credentials for the local MySQL and SQL Server databases to conect to

As an example:

````
petl:
  homeDir: "/home/mgoodrich/petl"
  datasourceDir: "/home/mgoodrich/openmrs/modules/config-pihemr/configuration/pi
h/petl/datasources"
  jobDir: "/home/mgoodrich/openmrs/modules/config-pihemr/configuration/pih/petl/
jobs"

mysql:
  host: "localhost"
  port: "3306"
  databaseName: "openmrs_mirebalais"
  user: "root"
  password: "***"

sqlserver:
  host: "localhost"
  port: "1433"
  databaseName: "openmrs_test"
  user: "sa"
  password: "******"

server:
  port: 9109

````

### Running

From the directory where you've created your application.yml file, run PETL via the following command.
(Note that the path to petl-2.1.0-SNAPSHOT.jar should be relative to the current directory you are in).

 java -jar target/petl-2.1.0-SNAPSHOT.jar 

# TODO:

* Add ability to load (initial + updates) to jobs and datasources from external location (eg. url)
* Add ability to check for updates to existing jobs and datasources from external location

* Provide web services and/or a web application that enables a broad range of users to:
** Check for updates to current configuration based on source + version
** Load updates to current configuration if available
** View the details of the various jobs and the history of their execution
** View the last update date and current status of the Pipeline(s)
** Kick off an update of the pipeline(s)
** View any errors in running the pipeline(s) 

