package org.pih.petl.job.config;

import com.github.dockerjava.api.model.Container;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.DockerConnector;
import org.pih.petl.PetlException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a data source configuration
 */
public class DataSource {

    private static Log log = LogFactory.getLog(DataSource.class);

    private String databaseType;
    private String host;
    private String port;
    private String databaseName;
    private String options;
    private String url; // Alternative to the above piecemeal settings
    private String user;
    private String password;
    private String containerName; // If this database is in a docker container, can configure the container name here

    //***** CONSTRUCTORS *****

    public DataSource() {}

    //***** INSTANCE METHODS *****

    /**
     * Gets a new Connection to the Data Source represented by this configuration
     * @return a connection to this DataSource
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

    /**
     * @return true if a connection is able to be successfully opened, false otherwise
     */
    public boolean testConnection(){
        try (Connection c = openConnection()) {
            DatabaseMetaData metadata = c.getMetaData();
            log.trace("Successfully connected to datasource: " + metadata.toString());
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * @param sql the sql update statement
     * @throws SQLException if an error occurs
     */
    public void executeUpdate(String sql) throws SQLException {
        try (Connection connection = openConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
            }
        }
    }

    /**
     * @param sql the sql query statement
     * @param  type the data type of the returned value
     * @param <T> the datatype of the type
     * @return the single value returned from the query
     * @throws SQLException if an error occurs
     */
    public <T> T querySingleValue(String sql, Class<T> type) throws SQLException {
        try (Connection connection = openConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return rs.getObject(1, type);
                    }
                    return null;
                }
            }
        }
    }

    /**
     * @param sql the sql query statement
     * @throws SQLException if an error occurs
     * @return the LocalDateTime returned from the given sql
     */
    public LocalDateTime queryAsLocalDateTime(String sql) throws SQLException {
        try (Connection connection = openConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return rs.getObject(1, LocalDateTime.class);
                    }
                    return null;
                }
            }
        }
    }

    /**
     * @param tableName the name of the table to check
     * @return true if the table exists, false otherwise
     * @throws SQLException if an error occurs
     */
    public boolean tableExists(String tableName) throws SQLException {
        try (Connection targetConnection = openConnection()) {
            return targetConnection.getMetaData().getTables(getDatabaseName(), null, tableName, new String[] {"TABLE"}).next();
        }
    }

    /**
     * @param tableName the name of the table to drop
     * @throws SQLException if an error occurs
     */
    public void dropTableIfExists(String tableName) throws SQLException {
        if (tableExists(tableName)) {
            executeUpdate("drop table " + tableName);
        }
    }

    /**
     * @param query the query to execute
     * @return a boolean result from this query
     * @throws SQLException if an error occurs
     */
    public boolean getBooleanResult(String query) throws SQLException {
        try (Connection sourceConnection = openConnection()) {
            boolean originalSourceAutoCommit = sourceConnection.getAutoCommit();
            sourceConnection.setAutoCommit(false);
            try (Statement statement = sourceConnection.createStatement()) {
                statement.execute(query);
                ResultSet resultSet = statement.getResultSet();
                resultSet.next();
                return resultSet.getBoolean(1);
            }
            finally {
                sourceConnection.rollback();
                sourceConnection.setAutoCommit(originalSourceAutoCommit);
            }
        }
    }

    /**
     * @param table the table for which to return the total number of rows
     * @return the total number of rows in the given table
     * @throws SQLException if an error occurs
     */
    public int rowCount(String table) throws SQLException {
        try (Connection connection = openConnection()) {
            QueryRunner qr = new QueryRunner();
            String query = "select count(*) from " + table;
            Number rowCount = qr.query(connection, query, new ScalarHandler<>());
            return rowCount.intValue();
        }
    }

    /**
     * @param tableName the table for which to return the columns
     * @return the columns in the given table
     * @throws SQLException if an error occurs
     */
    public List<TableColumn> getTableColumns(String tableName) throws SQLException {
        List<TableColumn> ret = new ArrayList<>();
        List<String> sizedTypes = Arrays.asList("VARCHAR", "CHAR", "DECIMAL");
        try (Connection targetConnection = openConnection()) {
            ResultSet rs = targetConnection.getMetaData().getColumns(getDatabaseName(), null, tableName, null);
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                if (sizedTypes.contains(type.toUpperCase())) {
                    String size = rs.getString("COLUMN_SIZE");
                    if (StringUtils.isNotEmpty(size)) {
                        type += "(" + size;
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

    /**
     * @return the jdbc url of the datasource
     */
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

    /**
     * If the datasource is configured with a Docker container name
     * this will check if the container is already started.  If it is not, it will attempt to start it and connect to it
     * If the result is that a new container is started and connected to successfully, this will return true
     * If no container is defined or if the container is already started, it will return false
     * If a connection to it fails, it will throw an exception
     * @return true if a container was actually started
     */
    public boolean startContainerIfNecessary() {
        boolean containerStarted = false;
        String containerName = getContainerName();
        if (StringUtils.isNotBlank(containerName)) {
            log.debug("Checking if container '" + containerName + "' is started");
            try (DockerConnector docker = DockerConnector.open()) {
                Container container = docker.getContainer(containerName);
                if (container != null) {
                    if (docker.isContainerRunning(container)) {
                        log.debug("Container '" + containerName + "' is already running");
                    }
                    else {
                        log.debug("Container '" + containerName + "' is not already running, starting it");
                        docker.startContainer(container);
                        containerStarted = true;
                        log.info("Container started: " + containerName);
                    }
                    log.debug("Testing for a successful database connection to  '" + containerName + "'");
                    // Wait up to 1 minute for the container to return a valid connection
                    int numSecondsToWait = 60;
                    while (numSecondsToWait >= 0) {
                        log.debug("Waiting for connection for " + numSecondsToWait + " seconds");
                        numSecondsToWait--;
                        Exception exception = null;
                        try {
                            if (testConnection()) {
                                log.debug("Connection to '" + containerName + "' established");
                                break;
                            }
                        }
                        catch (Exception e) {
                            exception = e;
                        }
                        if (numSecondsToWait == 0) {
                            throw new RuntimeException("Could not establish database connection to container " + containerName, exception);
                        }
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        }
                        catch (InterruptedException ignored) {}
                    }
                }
                else {
                    log.warn("No container named " + containerName + " found, skipping");
                }
            }
        }
        return containerStarted;
    }

    //***** PROPERTY ACCESS *****

    /**
     * @return the configured database type
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * @param databaseType the databaseType to set
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    /**
     * @return the configured host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return the configured port
     */
    public String getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(String port) {
        this.port = port;
    }

    /**
     * @return the configured database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @param databaseName the databaseName to set
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * @return the configured options
     */
    public String getOptions() {
        return options;
    }

    /**
     * @param options the options to set
     */
    public void setOptions(String options) {
        this.options = options;
    }

    /**
     * @return the configured url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * @return the configured user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the configured password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the configured container name
     */
    public String getContainerName() {
        return containerName;
    }

    /**
     * @param containerName the containerName to set
     */
    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }
}
