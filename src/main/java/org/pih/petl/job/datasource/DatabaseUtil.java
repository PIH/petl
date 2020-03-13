package org.pih.petl.job.datasource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.pih.petl.PetlException;

/**
 * Encapsulates a data source configuration
 */
public class DatabaseUtil {

    /**
     * Gets a new Connection to the Data Source represented by this configuration
     */
    public static Connection openConnection(EtlDataSource ds) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Class.forName("org.h2.Driver");
            return DriverManager.getConnection(ds.getJdbcUrl(), ds.getUser(), ds.getPassword());
        }
        catch (Exception e) {
            throw new PetlException("An error occured trying to open a connection to the database", e);
        }
    }

    /**
     * Close a given DB connection
     */
    public static void closeConnection(Connection connection) {
        DbUtils.closeQuietly(connection);
    }

    /**
     * Return the total number of rows in the given table for the given connection
     */
    public static Integer rowCount(Connection c, String table) throws SQLException {
        QueryRunner qr = new QueryRunner();
        String query = "select count(*) from " + table;
        return qr.query(c, query, new ScalarHandler<>());
    }
}
