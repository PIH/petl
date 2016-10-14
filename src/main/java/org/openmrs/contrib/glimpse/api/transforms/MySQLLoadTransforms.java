package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class MySQLLoadTransforms implements Serializable {


    @Autowired
    @Qualifier("openmrsDataSource")
    DataSource openmrsDataSource;


    // TODO load this from file and make configurable
    private String query = "select      p.patient_id,\n" +
            "            n.uuid as patient_uuid,\n" +
            "            n.gender,\n" +
            "            n.birthdate,\n" +
            "            n.birthdate_estimated,\n" +
            "            n.birthtime,\n" +
            "            n.dead,\n" +
            "            n.death_date,\n" +
            "            n.cause_of_death\n" +
            "from        patient p\n" +
            "inner join  person n on p.patient_id = n.person_id";


    // TODO should take in the name of the SQL transform to use?
    public PTransform<PBegin, PCollection<Map<String, Object>>> getTransform() {
        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(openmrsDataSource);

            PTransform<PBegin, PCollection<Map<String, Object>>> transform = JdbcIO.<Map<String, Object>>read().withDataSourceConfiguration(config).withQuery(query).withRowMapper(new JdbcIO.RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet resultSet) throws Exception {
                ResultSetMetaData metaData = resultSet.getMetaData();
                Integer columnCount = metaData.getColumnCount();
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), resultSet.getObject(i) != null ? resultSet.getObject(i) : new Object());
                }
                return row;
            }
        }).withCoder(MapCoder.of(StringUtf8Coder.of(), AvroCoder.of(Object.class)));

        return transform;
    }

}
