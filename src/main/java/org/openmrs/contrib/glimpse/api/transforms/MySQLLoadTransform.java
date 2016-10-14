package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.openmrs.contrib.glimpse.api.util.GlimpseUtils;
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
public class MySQLLoadTransform implements Serializable {


    @Autowired
    @Qualifier("openmrsDataSource")
    DataSource openmrsDataSource;

    public PTransform<PBegin, PCollection<Map<String, Object>>> getTransform(String sqlfile) {
        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(openmrsDataSource);

            String query = GlimpseUtils.readFile(sqlfile);

            PTransform<PBegin, PCollection<Map<String, Object>>> transform = JdbcIO.<Map<String, Object>>read().withDataSourceConfiguration(config).withQuery(query).withRowMapper(new JdbcIO.RowMapper<Map<String, Object>>() {

            @Override
            public Map<String, Object> mapRow(ResultSet resultSet) throws Exception {
                ResultSetMetaData metaData = resultSet.getMetaData();
                Integer columnCount = metaData.getColumnCount();
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), resultSet.getObject(i) != null ? resultSet.getObject(i) : "");
                }
                return row;
            }
        }).withCoder(MapCoder.of(StringUtf8Coder.of(), AvroCoder.of(Object.class)));

        return transform;
    }

}
