package org.pih.petl.job.config;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulates a data source configuration
 */
public class DataSourceConfig {

    private static Log log = LogFactory.getLog(DataSourceConfig.class);

    private String databaseType;
    private String host;
    private String port;
    private String databaseName;
    private String options;
    private String url; // Alternative to the above piecemeal settings
    private String user;
    private String password;

    //***** CONSTRUCTORS *****

    public DataSourceConfig() {}

    //***** INSTANCE METHODS *****

    /**
     * Gets a new Connection to the Data Source represented by this configuration
     */
    public Connection openConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Class.forName("org.h2.Driver");
            return DriverManager.getConnection(getJdbcUrl(), getUser(), getPassword());
        }
        catch (Exception e) {
            throw new PetlException("An error occured trying to open a connection to the database", e);
        }
    }

    public boolean testConnection() throws SQLException {
        try (Connection c = openConnection()) {
            DatabaseMetaData metadata = c.getMetaData();
            log.trace("Successfully connected to datasource: " + metadata.toString());
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public void executeUpdate(String sql) throws SQLException {
        try (Connection connection = openConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public boolean tableExists(String tableName) throws SQLException {
        try (Connection targetConnection = openConnection()) {
            return targetConnection.getMetaData().getTables(getDatabaseName(), null, tableName, new String[] {"TABLE"}).next();
        }
    }

    public void dropTableIfExists(String tableName) throws SQLException {
        if (tableExists(tableName)) {
            executeUpdate("drop table " + tableName);
        }
    }

    public int rowCount(String table) throws SQLException {
        try (Connection connection = openConnection()) {
            QueryRunner qr = new QueryRunner();
            String query = "select count(*) from " + table;
            return qr.query(connection, query, new ScalarHandler<>());
        }
    }

    public List<TableColumn> getTableColumns(String tableName) throws SQLException {
        List<TableColumn> ret = new ArrayList<>();
        List<String> sizedTypes = Arrays.asList("VARCHAR", "CHAR", "DECIMAL");
        try (Connection targetConnection = openConnection()) {
            ResultSet rs = targetConnection.getMetaData().getColumns(getDatabaseName(), null, tableName, null);
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                if (sizedTypes.contains(type)) {
                    String size = rs.getString("COLUMN_SIZE");
                    if (StringUtils.isNotEmpty(size)) {
                        type += " (" + size;
                        String decimalDigits = rs.getString("DECIMAL_DIGITS");
                        if (StringUtils.isNotEmpty(decimalDigits)) {
                            type += "," + decimalDigits;
                        }
                        type += ")";
                    }
                }
                ret.add(new TableColumn(name, type, null));
            }
        }
        return ret;
    }

    public String getJdbcUrl() {
        if (StringUtils.isNotBlank(url)) {
            return url;
        }
        else {
            StringBuilder sb = new StringBuilder();
            if ("mysql".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:mysql://").append(host).append(":").append(port);
                sb.append("/").append(databaseName).append("?");
                if (StringUtils.isNotBlank(options)) {
                    sb.append(options);
                }
                else {
                    sb.append("autoReconnect=true");
                    sb.append("&sessionVariables=default_storage_engine%3DInnoDB");
                    sb.append("&useUnicode=true");
                    sb.append("&characterEncoding=UTF-8");
                }
            }
            else if ("sqlserver".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:sqlserver://").append(host).append(":").append(port);
                sb.append(";").append("database=").append(databaseName);
                if (StringUtils.isNotBlank(options)) {
                    sb.append(";").append(options);
                }
            }
            else if ("h2".equalsIgnoreCase(databaseType)) {
                sb.append("jdbc:h2:").append(databaseName);
                if (StringUtils.isNotBlank(options)) {
                    sb.append(";").append(options);
                }
            }
            else {
                throw new PetlException("Currently only mysql, sqlserver, or h2 database types are supported");
            }
            return sb.toString();
        }
    }

    //***** PROPERTY ACCESS *****

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
