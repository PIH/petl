package org.openmrs.contrib.glimpse.api.transforms;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test of loading data out of the OpenMRS person and patient tables and into a new derived reporting table for patients
 * To set this up, you must first ensure the datasources listed below are configured correctly for your system.
 * Then, you must make sure the target database and table exists:
 *
 * create database reporting_test default charset utf8;
 * use reporting_test;
 * create table patient (uuid char(38) primary key not null, birthdate date, gender char(1));
 */
@Component
public class LoadPatientsFromOpenMRS implements Serializable {

    /**
     * Beam operates on a series of transforms in a pipeline.  To execute transforms sequentially, the output collection of one transform
     * must match the input collection of the next transform.
     *
     * The read transform is configured such that it is a "root transform", and thus has an input type of PBegin, which can be retrieved from the pipeline
     * It operates by executing a query, and for each row in the resultset, adding this as a Map<String, String> in the output collection
     *
     * I am not 100% sure yet how we could support returning values of differing datatypes as the values in the Map, given the available Coders.  TODO
     */
    public PTransform<PBegin, PCollection<Map<String, String>>> getReadTransform() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName("localhost");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("openmrs_neno");
        dataSource.setUser("root");
        dataSource.setPassword("root");

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(dataSource);

        String query = "select p.patient_id, n.uuid as uuid, n.gender, n.birthdate from patient p, person n where p.patient_id = n.person_id and p.voided = 0 and n.voided = 0";
        PTransform<PBegin, PCollection<Map<String, String>>> transform = JdbcIO.<Map<String, String>>read().withDataSourceConfiguration(config).withQuery(query).withRowMapper(new JdbcIO.RowMapper<Map<String, String>>() {
            @Override
            public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
                Map<String, String> row = new LinkedHashMap<>();
                row.put("patient_id", resultSet.getString("patient_id"));
                row.put("uuid", resultSet.getString("uuid"));
                row.put("birthdate", resultSet.getString("birthdate"));
                row.put("gender", resultSet.getString("gender"));
                return row;
            }
        }).withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        return transform;
    }

    /**
     * This transform takes as input a Collection of Map<String, String> which is what our read transform above produces.
     * It executes a prepared statement for each of these collection elements, and uses the Map as the data values in the sql substitution
     */
    public PTransform getWriteTransform() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName("localhost");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("reporting_test");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(dataSource);

        String writeStatement = "INSERT INTO patient (uuid, birthdate, gender) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE birthdate=?, gender=?";

        PTransform transform = JdbcIO.<Map<String, String>>write().withDataSourceConfiguration(config).withStatement(writeStatement).withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Map<String, String>>() {
            @Override
            public void setParameters(Map<String, String> data, PreparedStatement ps) throws Exception {
                ps.setString(1, data.get("uuid"));
                ps.setDate(2, getSqlDate(data.get("birthdate")));
                ps.setString(3, data.get("gender"));
                ps.setDate(4, getSqlDate(data.get("birthdate")));
                ps.setString(5, data.get("gender"));
            }
        });
        return transform;
    }

    /**
     * This simply creates a new Pipeline and wires it together, linking the output from the read transform to the input of the write transform
     */
    public void run() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(getReadTransform()).apply(getWriteTransform());
        p.run();
    }

    private static Date getSqlDate(String dateStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return new Date(df.parse(dateStr).getTime());
    }
}
