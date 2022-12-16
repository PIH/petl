# Flink Docker

This project exists to enable experimentation and testing with Flink containers running in a cluster, and with executing jobs via the SQL Client.

## Additional reading and demos

* [CDC With Flink SQL - Marta Paes](https://noti.st/morsapaes/QnBSAI/change-data-capture-with-flink-sql-and-debezium)
* [CDC With Flink SQL - Ververica](https://github.com/ververica/flink-sql-CDC)

## Dependencies 

This depends on the Kafka [event-log](../../event-log) running and available on the shared Docker network.

## Usage

**Download Libraries**

This project is designed to use the same version of Flink in the running Docker images as are being used as provided dependencies to the jobs.  There is a Maven pom in this folder that will download all (and more) of the libraries needed to execute jobs within the Flink cluster and in SQL Client.  The first step is to run this to download / update these libraries:

```shell
mvn clean install
```

The result of this should be a `target` directory that contains a large number of jar files.  The docker-compose file used in the next step will mount several of these as volumes into the Flink container to enable all of the necessary functionality.

**Start up Services**

```shell
docker-compose up -d
```

**Verify Running**

You should be able to view the web interface for this Flink cluster at:  [http://localhost:8081](http://localhost:8081).

**Try out the SQL Client**

Start up the SQL client by executing the following command:

```
docker-compose exec taskmanager ./bin/sql-client.sh
```

At the `Flink SQL>` prompt, start by creating a `person` table schema and connecting it to the Kafka person topic (streamed from the OpenMRS person table by Debezium)

```
CREATE TABLE person (
  person_id INT,
  uuid STRING,
  gender STRING,
  birthdate BIGINT,
  birthdate_estimated BOOLEAN,
  dead BOOLEAN,
  cause_of_death INT,
  cause_of_death_non_coded STRING,
  death_date BIGINT,
  death_date_estimated BOOLEAN,
  creator INT,
  date_created BIGINT,
  PRIMARY KEY (person_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'openmrs-humci.openmrs.person',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'connect-cluster-1',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset'
);
```

From here, you should be able to run normal SQL queries against this table and see the results in the SQL Client.  For example, try running:

```
select * from person;
```

or

```
select count(*) as num, gender from person group by gender;
```

**See streaming changes in real-time**

Run the last query above - `select count(*) as num, gender from person group by gender;`

Now, go into the OpenMRS database you are using as a source, and add or delete a patient, or update the gender of one or more patients.  As soon as you commit that change to the DB, you should notice the query results immediately change to reflect this.