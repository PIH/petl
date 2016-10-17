package org.openmrs.contrib.glimpse.api.transforms;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class MySQLLoadTransform implements Serializable {

    @Autowired
    @Qualifier("analysisDataSource")
    DataSource analysisDataSource;

    // TODO duplicate key update
    // TODO is avroencoding working?
    // TODO other columns, types

    public PTransform getTransform(String tableName, String primaryKey, List<String> columns) {

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(analysisDataSource);

        String writeStatement = "INSERT INTO " +
                tableName + " " +
                generateInsertParameters(primaryKey, columns); /*+ " " +
                "ON DUPLICATE KEY UPDATE " +
                generateUpdateParameters(columns);
*/
        PTransform transform = JdbcIO.<Map<String, Object>>write().withDataSourceConfiguration(config).withStatement(writeStatement).withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Map<String, Object>>() {
            @Override
            public void setParameters(Map<String, Object> data, PreparedStatement ps) throws Exception {
                addColumn(ps, data.get(primaryKey), 1);
                int i = 2;
                int j = 0;
                while (j < 1) {
                    for (String column : columns) {
                        Object val = data.get(column);
                        addColumn(ps, val, i);
                        i++;
                    }
                    j++;
                }
            }
        });
        return transform;
    }

    private String generateInsertParameters(String primaryKey, List<String> columns) {
        StringBuffer str = new StringBuffer();
        str.append("(" + primaryKey + ", ");
        str.append(StringUtils.collectionToDelimitedString(columns, ", "));
        str.append(") VALUES (");
        str.append(StringUtils.collectionToDelimitedString(Collections.nCopies(columns.size() + 1, "?"), ", "));
        str.append(")");
        return str.toString();
    }

    private String generateUpdateParameters(List<String> columns) {
        StringBuffer str = new StringBuffer();
        str.append(StringUtils.collectionToDelimitedString(columns, "=?,"));
        str.append("=?");
        return str.toString();
    }

    private void addColumn(PreparedStatement ps, Object val, int i) throws Exception {
        if (val instanceof Integer) {
            ps.setInt(i, ((Integer) val).intValue());
        }
        else if (val instanceof String) {
            ps.setString(i, (String) val);
        }
        // add date, etc
    }

    // for overriding in unit tests
    public void setAnalysisDataSource(DataSource analysisDataSource) {
        this.analysisDataSource = analysisDataSource;
    }
}
