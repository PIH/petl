PETL - PIH ETL Framework
================

This is an application whose goal is to support the execution of ETL jobs for PIH
Initially, this application  was written specifically to execute Pentaho Jobs and Transforms using Kettle.
However, it has since evolved to enable the execution of other types of Jobs, as well as to schedule these jobs.

# REQUIREMENTS:

* Java 8

# RELATED PROJECTS:

* PIH Puppet: https://github.com/PIH/mirebalais-puppet
* PIH-PENTAHO:  https://github.com/PIH/pih-pentaho
* PETL Ansible Scripts:  BitBucket PETL playbook/role

# OBJECTIVES:

* Provide a simple tool that can execution ETL transformations that are defined in external configuration files
* Provide an easy to understand and author syntax for authoring ETL jobs
* Enable support for Pentaho/Kettle but not require it, and do so without requiring the full PDI installation


# USAGE:

* Build project using maven (mvn clean package), using Java 8
* Run at the command line, or via running directly through Intellij


# TESTING WITH SQL SERVER:

### Create a Docker instance of SQL Server

Instructions can be found here:

SQL Server 2019: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-ver15&pivots=cs1-bash

SQL Server 2017: https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-linux-2017&pivots=cs1-bash

Note your password must be be at least 8 characters long and contain characters from three of the following four sets:
Uppercase letters, Lowercase letters, Base 10 digits, and Symbols,  or the docker container will fail silently. 
(You can run "docker logs [docker_id]" to debug)

Also note that the "root" user is named "sa", so you should set the username to "sa" when attempting to connect.


### Connect with Intellij or other client and create a new DB
`create database <YourDBName>;`

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

