PETL - PIH ETL Framework
================

This is a lightweight application that embeds the Pentaho libraries and is built to execute a pentaho pipeline and provide a set of wraparound services and user interfaces.

RELATED PROJECTS:
* PIH-PENTAHO:  https://github.com/PIH/pih-pentaho
* PETL Ansible Scripts:  BitBucket PETL playbook/role

REQUIREMENTS:

* Java 8
* Maven

OBJECTIVES:

* Simplify the deployment of the Pipeline, using technologies that we know well
* Provide a lighter footprint (the petl jar file is around 40MB currently.  Our Docker image for PDI is 1.23 GB)
* Automate some of the setup that should happen outside of a given Pipeline (eg. database creation, bookkeeping tables, scheduling of jobs)
* Provide web services and/or a web application that enables a broad range of users to:
** Enable, disable, and re-configure the various pipelines
** View the last update date and current status of the Pipeline(s)
** Kick off an update of the pipeline(s)
* View any errors in running the pipeline(s) 
* Provide a mechanism for integrating with DHIS2
**Thinking through whether we would have pipeline jobs that would load configuration and send data to DHIS2 via the REST API
* Potentially provide a platform for future reporting (TBD)
** Should a new Cohort Builder live here?
** Should any other Data Analysis tools live here?
** Should reports live here?  Should reporting frameworks simply use this as a source?

USAGE:

* Build project using maven (mvn clean package), using Java 8
* Run at the command line, or via running directly through Intellij

TODO:

* https://www.baeldung.com/docker-test-containers

CONFIGURATION:
* See PETL ansible scripts for expected installation, setup, and configuration settings
