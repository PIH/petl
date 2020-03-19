PETL - PIH ETL Framework
================

This is an application whose goal is to support the execution of ETL jobs for PIH
Initially, this application  was written specifically to execute Pentaho Jobs and Transforms using Kettle.
However, it has since evolved to enable the execution of other types of Jobs, as well as to schedule these jobs.

# REQUIREMENTS:

* Java 8

# RELATED PROJECTS:

* PIH-PENTAHO:  https://github.com/PIH/pih-pentaho
* PETL Ansible Scripts:  BitBucket PETL playbook/role

# OBJECTIVES:

* Provide a simple tool that can execution ETL transformations that are defined in external configuration files
* Provide an easy to understand and author syntax for authoring ETL jobs
* Enable support for Pentaho/Kettle but not require it, and do so without requiring the full PDI installation

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

# USAGE:

* Build project using maven (mvn clean package), using Java 8
* Run at the command line, or via running directly through Intellij

CONFIGURATION:
* See PETL ansible scripts for expected installation, setup, and configuration settings

# TESTING WITH SQL SERVER:

### Create a Docker instance of SQL Server
`
docker pull mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YOUR_DB_ROOT_PW_CHOICE>" -p 1433:1433 --name <YOUR_DB_CONTAINER_NAME> -d mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04
`
### Connect with Intellij or other client and create a new DB
`create database <YourDBName>;`
