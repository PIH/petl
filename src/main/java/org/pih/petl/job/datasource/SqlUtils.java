package org.pih.petl.job.datasource;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

/**
 * Encapsulates a data source configuration
 */
public class SqlUtils {

    /**
     * Return the total number of rows in the given table for the given connection
     */
    public static Integer rowCount(Connection c, String table) throws SQLException {
        QueryRunner qr = new QueryRunner();
        String query = "select count(*) from " + table;
        return qr.query(c, query, new ScalarHandler<>());
    }
}
