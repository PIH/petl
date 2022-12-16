# Trino

## Overview

Trino is a tool that provides a standard ANSI-SQL interface on top of a number of big data DBs that otherwise do not readily support this, as well as an ODBC/JDBC connector interface.  Many databases that have interesting performance characteristics worth exploring, like Cassandra, have dialects like CQL which are very close to SQL, but they are not exactly SQL.  Others, like ElasticSearch, have ways of executing SQL natively via submission of a SQL query to a REST endpoint, but which is not accessible via standard tools or approaches.  Trino is worth investigating as a means to make databases like Cassandra, ElasticSearch, Druid, and Hive/Parquet accessible to users and tools (eg. Power BI, SQL Reports) that are geared towards supporting ANSI-SQL and ODBC/JDBC connectivity.

This project aims to evaluate Trino and experiment with it.  It provides a dockerized environment with working configurations and starting database connections.  The [catalog](./config/catalog) folder contains an elasticsearch.properties file that is intended to work with the [elasticsearch](../elasticsearch) example in this repository and to demonstrate how one can use a JDBC-based database browser (eg. Intellij) to view the schema and execute queries using standard SQL.

## Setup

Setup follows the guides found below:

[Trino Docker Reference](https://github.com/trinodb/trino/blob/master/core/docker/README.md)
[Trino JDBC Information](https://trino.io/docs/current/installation/jdbc.html)
[Trino SSL / HTTPS configuration](https://trino.io/docs/current/security/tls.html)

* Start ElasticSearch by running the docker-compose setup in the parallel project.  Ideally you would have filled this with some OpenMRS data, using the [load-openmrs](../flink-jobs/load-openmrs) project.
* Start Trino by running docker-compose on this project:  `docker-compose up -d`
* Verify that the server is up and running with no errors in the logs:  `docker logs -f trino`
* Confirm you can log into the web UI (you will need to accept the self-signed certificate):  https://localhost:8443/ with credentials test/test
* Confirm that you can use the CLI:
  * `docker exec -it trino trino`
  * `trino> show catalogs` (Note: only elasticsearch and system appear because we are overriding the default catalogs shipped with the docker image)
  * `trino> show schemas from elasticsearch;`
  * `trino> use elasticsearch.default;`
  * `trino> show tables;`
  * `trino> select count(*) as num, gender from person_index group by gender;`

## Using JDBC

To demonstrate this, the test case was to be able to connect from a JDBC Client (Intellij) to ElasticSearch via JDBC.  By far the biggest difficulty with getting JDBC connectivity working was SSL.  Intellij consistently failed to connect due to error messages related to requirements that the communication be done via SSL, and that the Trino system itself was secured with a password.  This was fairly painful and time-consuming to figure out.  The process I managed to get working is described here.

### Initial Steps (reference for the future, not needed unless changing default configurations)

**Setup user/password for accessing Trino**

The current user and password are set up in this example with user/password of test/test.  These are the user credentials to log into the web UI.  To change these, we can follow these steps:

This involved (following the [Trino guide](https://trino.io/docs/current/security/password-file.html)):
* Adding `http-server.authentication.type=PASSWORD` to config.properties
* Adding `password-authenticator.properties` and `password.db`
* Populating `password.db` with the username and encrypted password to use.  I created an encrypted password with Apache using Docker:
  * Start up an Apache docker instance:  `docker run -d -p 8080:80 --name apache httpd:2.4` 
  * Get a bash shell in the container:  `docker exec -it apache bash`
  * Create a new password.db file:  `touch password.db`
  * Populate password.db with username and encrypted password:  `htpasswd -B -C 10 password.db test` (this is for user 'test'.  password is entered interactively)
  * Copy the contents of the password.db over to [this file](./config/password.db)

**Create self-signed certificate**

The following steps have been completed, but can be re-run as needed by following these steps.

* Generate a Certificate and Private Key file (trino.key and trino.crt):
```openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
   -keyout trino.key -out trino.crt -subj "/CN=example.com" \
   -addext "subjectAltName=DNS:localhost,DNS:*.localhost"
```

* Generate a [PEM File](https://trino.io/docs/current/security/inspect-pem.html)
```cat trino.key trino.crt > trino.pem```

* Optionally validate these files:
```
openssl rsa -in trino.pem -check -noout
openssl x509 -in trino.pem -text -noout
```

These 3 resulting files should replace the 3 files in the [ssl](./config/ssl) directory.

### Steps required for your JDBC client to succesfully connect

**Intellij**

First, make sure that the Java JRE that Intellij is using has the certificate installed into it's keystore:

* Determine what jre is being used to run Intellij `ps aux | grep Idea`
* For me this is currently `~/.local/share/JetBrains/Toolbox/apps/IDEA-U/ch-0/213.6461.79/jbr/bin/java`.  Call this <javadir>.
* Run from the directory where the trino.crt file is located:
`<javadir>/bin/keytool -importcert -trustcacerts -alias trino -file trino.crt -keystore <javadir>/lib/security/cacerts -storepass changeit`
* If you ever need to delete this certificate in order to add a new one:
`<javadir>/bin/keytool -delete -trustcacerts -alias trino -keystore <javadir>/lib/security/cacerts -storepass changeit`

Now, you should be able to configure a new datasource.  Note, you must **manually** add the `?SSL=true` parameter to the end of the JDBC URL to get this to work.

* host:  `localhost`
* port:  `8443`
* database:  `elasticsearch/default`
* url: `jdbc:trino://localhost:8443/elasticsearch/default?SSL=true`