package org.pih.petl.job.datasource;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.pih.petl.PetlException;

/**
 * Encapsulates a data source configuration
 */
public class EtlConnectionManager {

    /**
     * @return true if the type of data source is MySQL
     */
    public static boolean isMysql(EtlDataSource ds) {
        return "mysql".equals(ds.getDatabaseType());
    }

    /**
     * @return true if the type of data source is MS SqlServer
     */
    public static boolean isSqlServer(EtlDataSource ds) {
        return "sqlserver".equals(ds.getDatabaseType());
    }

    /**
     * @return true if the type of data source is H2
     */
    public static boolean isH2(EtlDataSource ds) {
        return "h2".equals(ds.getDatabaseType());
    }

    /**
     * Gets a new Connection to the Data Source represented by this configuration
     */
    public static Connection openConnection(EtlDataSource ds) {
        try {
            StringBuilder sb = new StringBuilder();
            // MySQL DB
            if (isMysql(ds)) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                sb.append("jdbc:mysql://").append(ds.getHost()).append(":").append(ds.getPort());
                sb.append("/").append(ds.getDatabaseName()).append("?");
                if (StringUtils.isNotBlank(ds.getOptions())) {
                    sb.append(ds.getOptions());
                }
                else {
                    sb.append("autoReconnect=true");
                    sb.append("&sessionVariables=default_storage_engine%3DInnoDB");
                    sb.append("&useUnicode=true");
                    sb.append("&characterEncoding=UTF-8");
                }
                return DriverManager.getConnection(sb.toString(), ds.getUser(), ds.getPassword());
            }
            // SQL Server DB
            else if (isSqlServer(ds)) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                sb.append("jdbc:sqlserver://").append(ds.getHost()).append(":").append(ds.getPort());
                sb.append(";").append("database=").append(ds.getDatabaseName());
                sb.append(";").append("user=").append(ds.getUser());
                sb.append(";").append("password=").append(ds.getPassword());
                if (StringUtils.isNotBlank(ds.getOptions())) {
                    sb.append(";").append(ds.getOptions());
                }
                return DriverManager.getConnection(sb.toString());
            }
            // H2 Database
            else if (isH2(ds)) {
                Class.forName("org.h2.Driver");
                sb.append("jdbc:h2:").append(ds.getDatabaseName());
                if (StringUtils.isNotBlank(ds.getOptions())) {
                    sb.append(";").append(ds.getOptions());
                }
                return DriverManager.getConnection(sb.toString(), ds.getUser(), ds.getPassword());
            }
            else {
                throw new PetlException("You must specify a databaseType of 'mysql', 'sqlserver', or 'h2'");
            }
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
}
