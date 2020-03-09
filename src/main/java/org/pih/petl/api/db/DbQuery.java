package org.pih.petl.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.StringUtils;
import org.pih.petl.api.config.EtlDataSource;

/**
 * Encapsulates a data source configuration
 */
public class DbQuery {

    /**
     * Return the total number of rows in the given table for the given connection
     */
    public static Integer rowCount(Connection c, String table) throws SQLException {
        QueryRunner qr = new QueryRunner();
        String query = "select count(*) from " + table;
        return qr.query(c, query, new ScalarHandler<>());
    }
}
