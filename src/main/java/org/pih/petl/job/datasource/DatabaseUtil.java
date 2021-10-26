package org.pih.petl.job.datasource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Encapsulates a data source configuration
 */
public class DatabaseUtil {

    private static Log log = LogFactory.getLog(DatabaseUtil.class);

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
            log.error(e);
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

    public static void dropTable(Connection c, String table) throws SQLException {
        QueryRunner qr = new QueryRunner();
        qr.update(c, "IF OBJECT_ID('dbo." + table + "') IS NOT NULL DROP TABLE dbo." + table);
    }

    public static Boolean tableExists(Connection c, String table) throws SQLException {
        QueryRunner qr = new QueryRunner();
        String query = "IF OBJECT_ID ('dbo." + table + "') IS NOT NULL SELECT 1 ELSE SELECT 0";
        int result = qr.query(c, query, new ScalarHandler<>());
        return result == 1;
    }
}


