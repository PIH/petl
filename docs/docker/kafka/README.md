# OpenMRS Event Log

## Overview

This describes a setup process for setting up an event log for OpenMRS.  The heart of this event log is[Apache Kafka](https://kafka.apache.org/), which provides an immutable commit log and streaming platform for events, and contains a library of available connectors that support publishing and subscribing to a variety of systems.

The primary source of events feeding the Kafka instance is the OpenMRS MySQL instance.  This is accomplished through the [Debezium](https://debezium.io/documentation/reference/stable/index.html) platform.  Debezium is a change-data-capture (CDC) library that consumes the MySQL bin log and produces a stream of database change events, including all inserts, updates, and deletes, as well as schema changes.  Debezium is connected to Kafka via a Debezium connector library that is added to the [Kafka Connect](https://kafka.apache.org/documentation/#connect) service and configured to publish event streams of [specific tables within the OpenMRS database](openmrs-connector.json) to individual Kafka topics.

This depends on the [mysql](../mysql) container that has row-level bin logging enabled.  MySQL 8 enables bin logging by default.

There are several services and components to this platform that can be seen in the provided [docker-compose.yml](docker-compose.yml) file.  These are as follows:

* Kafka.      The Event Streaming platform
* Zookeeper.  Used by Kafka for management of configuration, naming, and other services across distributed cluster.  May be eliminated in the near future.  See [Zookeeper Docs](https://zookeeper.apache.org/)
* Connect.    The Kafka Connect service serves to connect various data sources and sinks to Kafka.  The version used here comes pre-configured with a Debezium connector.
* Kowl.       A UI for viewing Kafka topics and useful for seeing the data that is being produced by Debezium from MySQL

Much of the setup and configuration seen here followed these resource guides:

* [Debezium Tutorial](https://debezium.io/documentation/reference/stable/tutorial.html)
* [Mounting External Volumes for Kafka and Zookeeper](https://docs.confluent.io/platform/current/installation/docker/operations/external-volumes.html)
* [Docker Documentation in Github for Debezium Kafka and Zookeeper Images](https://github.com/debezium/docker-images)
* [Docker Documentation in Dockerhub for for Debezium Kafka and Zookeeper Images](https://hub.docker.com/r/debezium)
* [Ampath OpenMRS ELT project](https://github.com/kimaina/openmrs-elt/tree/master/cdc)

## Additional Reading and Helpful Links and Tutorials

* [Kafka Presentations by Robin Moffatt](https://rmoff.net/2020/09/23/a-collection-of-kafka-related-talks/)
* [Debezium Documentation](https://debezium.io/documentation/reference/stable/index.html)
* [Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)


## Installation

* Create a new directory to use for this project.  All further commands and instructions are relative to this folder.
* Copy or create symbolic links between [docker-compose.yml](docker-compose.yml) and [openmrs-connector.json](openmrs-connector.json) to this folder
* Setup or identify existing MySQL database with row-level bin logging enabled.  See [openmrs-mysql](../mysql) for examples.
* Modify the docker-compose configuration as needed to reflect specifics of the MySQL instance

### Saving data across restarts

The supplied docker-compose file requires 3 directories to exist in the project directory, relative to the location of the above-installed docker-compose file.  These directories need to be writable by the "kafka" user/group (identified by uid 1001) from the Docker containers.  These directories allow the Zookeeper and Kafka topics, data, and configuration to be persisted across container restarts.

If you just want to start things up and see how they work, and don't want to persist any data in Kafka across restarts, you can remove the volume specifications for "kafka" and "zookeeper" from the docker-compose file.

If you do want to support persistent storage, you need to pre-create these directories on the host with a suitable user/group.  If the directories do not already exist on the host when docker-compose starts up, it will create them as the root user, and they will not be accessible to the user within the container, and startup will fail.

```shell
sudo groupadd -r kafka -g 1001
sudo useradd -u 1001 -r -g kafka -s /sbin/nologin -c "Kafka user" kafka
mkdir -p kafka/data
mkdir -p zookeeper/data
mkdir -p zookeeper/txns
sudo chown -R kafka: kafka/
sudo chown -R kafka: zookeeper/
```

When all of the services have started up, one will see files accumulate in these directories.  Monitoring the size of these directories and the files within them is something that will be of particular interest, especially when testing or running against a large production-scale system.

## Startup and Configuration

Fire up all of the services via docker-compose:

```shell
docker-compose up -d
```

Configure Kafka by interacting with the Kafka Connect REST endpoint.  You should modify the values in [openmrs-connector.json](openmrs-connector.json) to match your environment.

You can view a list of all connectors configured (will be initially empty):

```shell
curl -H "Accept:application/json" localhost:8083/connectors/
```

You can add a new OpenMRS source connector:

```shell
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @openmrs-connector.json
```

You can view the details of a connector that is already added:

```shell
curl -i -X GET -H "Accept:application/json" localhost:8083/connectors/openmrs-connector
```

The above connector registration only needs to be done at first installation, if the services are run with persistent volumes.  If the volumes are removed from the docker-compose configuration, then each time the Kafka data will reset, and the connector will need to be re-added, topics will be recreated, and events will re-import from Debezium/MySQL.

## Monitoring the Event Log

You can fire up [Kowl](http://localhost:8282/), which is a web UI for managing the Kafka instance, to view the event log.

## Important Notes and Future Considerations

### Handling of Date, Datetime, Timestamp, and Time columns

The Debezium MySQL connector appears to represent all date/time-based data as numbers related to the Unix Epoch (i.e. a number relative to Midnight on January 1, 1970.  If the date/time is before this time, it will be a negative number, after this time will be a positive number.

For "Date" columns, these are stored as "Number of days before/after the Epoch", as Date columns have no time component. For example, a birthdate stored in the OpenMRS person table as "May 27, 2007" will appear in the Kafka event log in a Debezium message as `"birthdate": 13660`.  This is because May 27, 2007 is 13660 days after Jan 1, 1970.  Specifically, the Unix Epoch time of midnight on May 27, 2007 = 13660 days * (60*60*24) seconds per day = 1180224000.

For "Datetime" columns, these are stored as "Number of milliseconds before/after the Epoch".  By default, MySQL columns of type "datetime" have a precision of 0, meaning they are precise only to the second, which means all such values will have 000 as their last three number.  MySQL is capable of storing datetime with higher levels of precision (eg. datetime(3) will store millisecond-level precision), however this is not something that is generally found in OpenMRS.  Datetime columns are converted into epoch milliseconds or microseconds based on the column’s precision by using UTC.  As an example, a date_created stored in the OpenMRS person table as "2022-01-19 03:21:03" will appear in the Kafka event log in a Debezium message as `"date_created":  1642562463000"`.  This is the Epoch representation.

For "Time" columns, these are stored as the number of microseconds since midnight.

More information can be found [in the Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types).

#### Downstream Considerations

The above representations of dates and times have advantages and disadvantages that need to be weighed.  It's possible that the advantages include performance and accuracy.  However, there are clear disadvantages.  Most obvious is readability - one cannot easily look at the Kafka message log and understand what the dates and times mean.  Beyond this however, there are downstream consumption issues as well, namely when trying to consume these messages with the Flink SQL API, as the Flink JSON Deserializer does not seem to support Epoch-based values for dates and timestamps.  Rather, in order to stream the Debezium date/time columns into a Flink SQL Table, I was only able to do this by defining these datatypes as `BIGINT`.  A specific example is as follows:

The following fails:

```
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("" +
                "CREATE TABLE person (\n" +
                "  person_id INT,\n" +
                "  birthdate DATE,\n" +
                "  date_created TIMESTAMP,\n" +
                "  PRIMARY KEY (person_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'openmrs-humci.openmrs.person',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = '1',\n" +
                "    'format' = 'debezium-json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        TableResult numPersons = tEnv.executeSql("select * from person");
        numPersons.print();
```

The resulting errors look like the following:

```
Caused by: java.time.format.DateTimeParseException: Text '1104537600000' could not be parsed at index 0
	at java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:1949)
	at java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1777)
	at org.apache.flink.formats.json.JsonToRowDataConverters.convertToTimestamp(JsonToRowDataConverters.java:225)
```

or 

```
Caused by: java.time.format.DateTimeParseException: Text '-13107' could not be parsed at index 6
	at java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:1949)
	at java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1777)
	at org.apache.flink.formats.json.JsonToRowDataConverters.convertToDate(JsonToRowDataConverters.java:209)
```

However, if in the above example, the DATE and TIMESTAMP columns are defined as BIGINT, then this succeeds.

#### Possible Approaches

**Keep as BIGINT**

The easiest initial approach is to keep these as BIGINT columns until a need arises for these to be modeled differently.  Depending on our sink datasource, this may not be an issue.  For example, I believe that in ElasticSearch, one can specify dates and timestamps in a number of formats, including Epoch time.

**Try alternative formats like Avro**

It is possible that switching from Json deserialization to Avro serialization or Json-schema may fix this, if the deserialization is sufficiently smarter and/or is able to use the attached schema information to parse the date and time from the Epoch if provided.

**Propose a fix to the Flink JSON deserializer**

Report the above errors and use case to the Flink development team, possibly issue a PR, and see if this is something that might be able to be fixed in a later version of Flink.  This implies that the current behavior is a bug, which may or may not be the case.

**Add an SMT (single message transform) using the TimestampConverter**

I could not get this to work when attempted with the Flink SQL API, but may be worth additional testing.

* [Single Message Transforms](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
* [TimestampConverter](https://docs.confluent.io/platform/current/connect/transforms/timestampconverter.html#timestampconverter)
* [Demo/Tutorial of TimestampConverter](https://github.com/confluentinc/demo-scene/blob/master/kafka-connect-single-message-transforms/day8.adoc)

**Add a custom Datatype converter to Kafka Connect**

An approach to this is described in this [debezium-datetime-converter](https://github.com/holmofy/debezium-datetime-converter) project, I was able to get this to work successfully enough to confirm that this is a viable appoach.  One can test this out by:

* Downloading the [latest release](https://github.com/holmofy/debezium-datetime-converter/releases) jar file
* Mounting this jar file into the "connect" container withing the `/kafka/connect/debezium-connector-mysql` directory
```
volumes:
  - "./debezium-extension-1.4.0.Final.jar:/kafka/connect/debezium-connector-mysql/debezium-extension-1.4.0.Final.jar"
```
* Modifying the [openmrs-connector](openmrs-connector.json) to configure this converter:
```
    "converters": "datetime",
    "datetime.type": "com.darcytech.debezium.converter.MySqlDateTimeConverter",
    "datetime.format.date": "yyyy-MM-dd",
    "datetime.format.time": "HH:mm:ss",
    "datetime.format.datetime": "yyyy-MM-dd HH:mm:ss",
    "datetime.format.timestamp": "yyyy-MM-dd HH:mm:ss",
    "datetime.format.timestamp.zone": "UTC+8"
```

In my testing, this successfully converted all of my "birthdate" columns to "yyyy-MM-dd" format.  However, it also converted all of my "date_created" columns to null.  So if we take this approach, we likely want to build and deploy our own custom Converter jar into Kafka connect.  However, this is simply a single class, and should not be problematic.  See [example from above](https://github.com/holmofy/debezium-datetime-converter/blob/master/src/main/java/com/darcytech/debezium/converter/MySqlDateTimeConverter.java)


### Docker Images and Deployment

Depending on how this is adopted and scaled, replacing the Debezium-published Docker images with our own images that are based on official images and contain the connectors and configuration needed may be desirable down the road.  This may also give us the ability to package own custom connectors more easily.

We may also want to consider more of a supported Kafka platform, like the [Open Source Confluent Platform](https://www.confluent.io/product/confluent-platform/)

### Switching to Avro

I did some initial spiking on swapping out JSON for Avro as the serialization mechanism, but after hitting errors I decided to leave this for a later date so as not to complicate the setup.  There are pros and cons to Avro.  The pros are that it is significantly smaller, and more efficient, and that it has a built-in schema that can be utilized in message consumption.  The cons are that it is more complicated to setup, requires the addition of another service (the schema registry), and the messages themselves require an Avro-enabled reader to interpret (they are binary format).  Documenting below the steps I took to attempt the switch to Avro:

**Update openmrs-connector.json:**
```
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schemaregistry:8085",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schemaregistry:8085",
    "internal.key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "internal.value.converter": "org.apache.kafka.connect.json.JsonConverter",
```

**Add schemaregistry to docker-compose.yml:**

```yaml
  # The schema registry is required in order to use Avro as the serialization format for events by Debezium
  schemaregistry:
    image: confluentinc/cp-schema-registry:7.0.1
    container_name: "schema-registry"
    ports:
    - "8085:8085"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schemaregistry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8085
    depends_on:
    - "zookeeper"
    - "kafka"
```

**Add schemaregistry configuration to kowl in docker-compose.yml:**

This is needed in order for Kowl to be able to access the Avro schema in the registry, and to decode the messages for visualization.

```yaml
    environment:
      KAFKA_BROKERS: "kafka:9092"
      KAFKA_SCHEMAREGISTRY_ENABLED: "true"
      KAFKA_SCHEMAREGISTRY_URLS: "http://schemaregistry:8085"
    depends_on:
      - "kafka"
      - "schemaregistry"
```

**Update downstream jobs**

Replace json with avro in pom dependencies and add confluent repository to repositories:

```
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-avro-confluent-registry</artifactId>
            <version>${flink.version}</version>
        </dependency>
        ...
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
```

Update Kafka connector configurations for Table API like follows:

```
    "    'format' = 'debezium-avro-confluent',\n" +
    "    'debezium-avro-confluent.schema-registry.url' = 'http://localhost:8085',\n" +
```