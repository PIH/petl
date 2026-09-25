package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.DockerConnector;
import org.pih.petl.LogUtils;
import org.pih.petl.PetlException;
import org.pih.petl.PhaseTimer;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.config.TableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PetlJob that can load into SQL Server table
 */
@Component("sqlserver-bulk-import")
public class SqlServerImportJob implements PetlJob {

    private final Log log = LogFactory.getLog(getClass());

    private final Map<String, Object> stagingMonitors = new ConcurrentHashMap<>();
    private final Map<String, Object> tableMonitors = new ConcurrentHashMap<>();

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {

        log.debug("Executing SqlServerImportJob");
        PhaseTimer timer = new PhaseTimer();
        timer.start("setup");
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        List<String> containersStarted = new ArrayList<>();
        String source = configReader.getString("extract", "datasource");
        int progressIntervalSeconds = configReader.getInt(300, "load", "bulkCopy", "progressIntervalSeconds");
        ImportProgressMonitor progressMonitor = new ImportProgressMonitor(timer, progressIntervalSeconds);
        try {
            // Get source datasource
            DataSource sourceDatasource = configReader.getDataSource("extract", "datasource");
            if (sourceDatasource.startContainerIfNecessary()) {
                containersStarted.add(sourceDatasource.getContainerName());
            }
            String extractConnectionError = sourceDatasource.getConnectionError();
            if (extractConnectionError != null) {
                throw new PetlException("Unable to connect to datasource: " + configReader.getString("extract", "datasource") +
                        " (" + sourceDatasource.describe() + "): " + extractConnectionError);
            }

            // Get any conditional, and execute against the source datasource.  If this returns false, skip execution
            String conditional = configReader.getString("extract", "conditional");
            if (StringUtils.isNotEmpty(conditional)) {
                if (!sourceDatasource.getBooleanResult(conditional)) {
                    log.debug("Conditional returned false, skipping");
                    return;
                }
            }

            // Get source query
            String sourceQuery = configReader.getRequiredFileContents("extract", "query");
            String sourceContextStatements = configReader.getFileContents("extract", "context");
            if (sourceContextStatements != null) {
                sourceQuery = sourceContextStatements + System.lineSeparator() + sourceQuery;
            }

            // Get target datasource
            DataSource targetDatasource = configReader.getDataSource("load", "datasource");
            if (targetDatasource.startContainerIfNecessary()) {
                containersStarted.add(targetDatasource.getContainerName());
            }
            String loadConnectionError = targetDatasource.getConnectionError();
            if (loadConnectionError != null) {
                throw new PetlException("Unable to connect to datasource: " + configReader.getString("load", "datasource") +
                        " (" + targetDatasource.describe() + "): " + loadConnectionError);
            }

            // Get target table name
            String targetTable = configReader.getString("load", "table");

            // Get target table schema
            String targetSchema = configReader.getFileContents("load", "schema");

            // Get extra columns to add to schema and import
            List<TableColumn> extraColumns = new ArrayList<>();
            for (JsonNode extraColumnNode : configReader.getList("load", "extraColumns")) {
                TableColumn tableColumn = new TableColumn();
                tableColumn.setName(configReader.getString(extraColumnNode.get("name")));
                tableColumn.setType(configReader.getString(extraColumnNode.get("type")));
                tableColumn.setValue(configReader.getString(extraColumnNode.get("value")));
                extraColumns.add(tableColumn);
            }
            if (!extraColumns.isEmpty()) {
                if (targetSchema == null) {
                    throw new PetlException("Extra Columns can only be specified when a specific schema is loaded");
                } else {
                    targetSchema = SqlUtils.addExtraColumnsToSchema(targetSchema, extraColumns);
                }
            }

            boolean dropAndRecreate = configReader.getBoolean(true, "load", "dropAndRecreateTable");

            boolean usePartitioning = false;

            // Get partition information
            String partitionScheme = configReader.getString("load", "partition", "scheme");
            String partitionColumn = configReader.getString("load", "partition", "column");
            String partitionValue = configReader.getString("load", "partition", "value");

            if (StringUtils.isNotEmpty(partitionScheme) || StringUtils.isNotEmpty(partitionColumn)) {
                if (targetSchema == null) {
                    throw new PetlException("Partition scheme and column can only be specified when a specific schema is loaded");
                } else if (StringUtils.isEmpty(partitionScheme)) {
                    throw new PetlException("You must specify a partition scheme if you specify a partition column");
                } else if (StringUtils.isEmpty(partitionColumn)) {
                    throw new PetlException("You must specify a partition column if you specify a partition scheme");
                } else {
                    targetSchema = SqlUtils.addPartitionSchemeToSchema(targetSchema, partitionScheme, partitionColumn);
                    usePartitioning = true;
                }
            }
            if (usePartitioning && StringUtils.isEmpty(partitionValue)) {
                throw new PetlException("You must specify a value for your partition column");
            }

            boolean incremental = configReader.getBoolean(false, "load", "partition", "incremental", "enabled");
            LocalDateTime newWatermark = null;
            LocalDateTime previousWatermark = null;
            String newWatermarkQuery = configReader.getFileContents("load", "partition", "incremental", "newWatermarkQuery");
            String previousWatermarkQuery = configReader.getFileContents("load", "partition", "incremental", "previousWatermarkQuery");
            String incrementalDeleteStatement = configReader.getFileContents("load", "partition", "incremental", "deleteStatement");
            String updateWatermarkStatement = configReader.getFileContents("load", "partition", "incremental", "updateWatermarkStatement");

            if (incremental) {
                log.debug("Incremental loading is enabled for this job");
                if (!usePartitioning) {
                    throw new PetlException("You must use partitioning to do incremental loading from a watermark");
                }
                if (StringUtils.isBlank(incrementalDeleteStatement)) {
                    throw new PetlException("You must specify an incremental deleteStatement if incremental loading is enabled");
                }
                if (StringUtils.isBlank(newWatermarkQuery)) {
                    throw new PetlException("You must specify an incremental newWatermarkQuery to retrieve existing watermark from target");
                }
                if (StringUtils.isBlank(previousWatermarkQuery)) {
                    throw new PetlException("You must specify an incremental previousWatermarkQuery to retrieve existing watermark from target");
                }
                if (StringUtils.isBlank(updateWatermarkStatement)) {
                    throw new PetlException("You must specify an incremental updateWatermarkStatement to track watermarks");
                }

                try {
                    newWatermark = targetDatasource.queryAsLocalDateTime(newWatermarkQuery);
                    log.debug("New watermark value: " + newWatermark);
                } catch (Exception e) {
                    throw new PetlException("Error trying to retrieve a new watermark value", e);
                }

                try {
                    previousWatermark = targetDatasource.queryAsLocalDateTime(previousWatermarkQuery);
                    log.debug("Previous watermark value: " + previousWatermark);
                } catch (Exception e) {
                    log.warn("Error retrieving previous watermark", e);
                }

                if (newWatermark != null && newWatermark.equals(previousWatermark)) {
                    log.info("Skipping import, no changes since previous watermark: " + previousWatermark);
                    return;
                }
                if (previousWatermark != null) {
                    log.info("Incremental load from watermark " + previousWatermark + " to " + newWatermark);
                }

                String mysqlWatermarks = "" +
                        "set @newWatermark = " + SqlUtils.mysqlDate(newWatermark) + ";" + System.lineSeparator() +
                        "set @previousWatermark = " + SqlUtils.mysqlDate(previousWatermark) + ";" + System.lineSeparator();

                String sqlServerWatermarks = "" +
                        "DECLARE @newWatermark DATETIME = " + SqlUtils.sqlServerDate(newWatermark) + ";" + System.lineSeparator() +
                        "DECLARE @previousWatermark DATETIME = " + SqlUtils.sqlServerDate(previousWatermark) + ";" + System.lineSeparator();

                // Ensure that the source incremental extract query has access to the watermarks
                sourceQuery = mysqlWatermarks + sourceQuery;

                // Ensure that the target incremental update watermark statement has access to the watermarks
                updateWatermarkStatement = sqlServerWatermarks + updateWatermarkStatement;

                // Ensure that the target incremental delete query has access to the watermarks
                incrementalDeleteStatement = sqlServerWatermarks + incrementalDeleteStatement;
            }

            String tableToBulkInsertInto = usePartitioning ? targetTable + "_" + partitionValue : targetTable;
            Object stagingMonitor = stagingMonitors.computeIfAbsent(tableToBulkInsertInto, k -> new Object());
            Object tableMonitor = tableMonitors.computeIfAbsent(targetTable, k -> new Object());

            // Only one import at a time may load a given staging table (partitioned imports) or target table (otherwise).
            // Partitioned imports into the same target table run concurrently, only holding the target table lock while
            // checking its schema, reading its existing data for incremental loads, and switching in their partition
            timer.start("lock wait");
            synchronized (stagingMonitor) {
                Integer rowsBeforeImport = null;
                if (usePartitioning) {
                    synchronized (tableMonitor) {
                        timer.start("staging setup");
                        targetDatasource.dropTableIfExists(tableToBulkInsertInto);
                        String partitionSchema = SqlUtils.addSuffixToCreatedTablename(targetSchema, "_" + partitionValue);
                        targetDatasource.executeUpdate(partitionSchema);
                        dropAndRecreateIfSchemasDiffer(targetDatasource, targetTable, tableToBulkInsertInto, targetSchema);

                        // If we are doing incremental loading, we first need to pre-populate the partition table with existing data
                        if (incremental && previousWatermark != null) {
                            timer.start("incremental prep");
                            log.debug("Inserting existing values from target table");
                            String insertSql = "insert into " + tableToBulkInsertInto + " select * from " + targetTable + " where " + partitionColumn + " = " + partitionValue;
                            log.trace(insertSql);
                            targetDatasource.executeUpdate(insertSql);
                            logNumberOfRows("After Insert:", targetDatasource, tableToBulkInsertInto);
                            log.debug("Deleting values that have changed since the last watermark");
                            log.trace(incrementalDeleteStatement);
                            targetDatasource.executeUpdate(incrementalDeleteStatement);
                            rowsBeforeImport = logNumberOfRows("After Delete:", targetDatasource, tableToBulkInsertInto);
                        }
                    }
                } else {
                    timer.start("staging setup");
                    if (StringUtils.isNotEmpty(targetSchema)) {
                        if (dropAndRecreate) {
                            log.debug("Dropping existing table: " + tableToBulkInsertInto);
                            targetDatasource.dropTableIfExists(tableToBulkInsertInto);
                        }
                        if (!targetDatasource.tableExists(tableToBulkInsertInto)) {
                            log.debug("Creating target schema for: " + tableToBulkInsertInto);
                            targetDatasource.executeUpdate(targetSchema);
                        } else {
                            log.debug("Target table already exists at: " + tableToBulkInsertInto);
                        }
                    } else {
                        log.debug("No target schema specified");
                    }
                }
                if (incremental && previousWatermark == null) {
                    log.info("No previous watermark found, performing full load up to watermark " + newWatermark);
                }

                    // Get bulk load configuration
                    int batchSize = configReader.getInt(100, "load", "bulkCopy", "batchSize");
                    int timeout = configReader.getInt(7200, "load", "bulkCopy", "timeout"); // 2h default
                    boolean testOnly = configReader.getBoolean(false, "load", "bulkCopy", "testOnly");

                    try (Connection sourceConnection = sourceDatasource.openConnection()) {
                        try (Connection targetConnection = targetDatasource.openConnection()) {

                            boolean originalSourceAutoCommit = sourceConnection.getAutoCommit();
                            boolean originalTargetAutocommit = targetConnection.getAutoCommit();

                            try {
                                sourceConnection.setAutoCommit(false); // We intend to rollback changes to source after querying DB
                                targetConnection.setAutoCommit(true);  // We want to commit to target as we go, to query status

                                // Now execute a bulk import
                                log.debug("Executing import");

                                // Parse the source query into statements
                                List<String> stmts = SqlUtils.parseSqlIntoStatements(sourceQuery, ";");
                                log.trace("Parsed extract query into " + stmts.size() + " statements");

                                // Iterate over each statement, and execute.  The final statement is expected to select the data out.
                                for (Iterator<String> sqlIterator = stmts.iterator(); sqlIterator.hasNext(); ) {
                                    String sqlStatement = sqlIterator.next();
                                    Statement statement = null;
                                    try {
                                        log.trace("Executing: " + sqlStatement);
                                        StopWatch sw = new StopWatch();
                                        sw.start();
                                        if (sqlIterator.hasNext()) {
                                            timer.start("extract prep");
                                            statement = sourceConnection.createStatement();
                                            statement.execute(sqlStatement);
                                            log.trace("Statement executed");
                                        } else {
                                            log.trace("This is the last statement, treat it as the extraction query");

                                            sqlStatement = SqlUtils.addExtraColumnsToSelect(sqlStatement, extraColumns);
                                            log.trace("Executing SQL extraction");
                                            log.trace(sqlStatement);

                                            statement = sourceConnection.prepareStatement(
                                                    sqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                                            );
                                            if ("mysql".equals(sourceDatasource.getDatabaseType())) {
                                                statement.setFetchSize(Integer.MIN_VALUE);
                                            }

                                            ResultSet resultSet = null;
                                            try {
                                                timer.start("query to first row");
                                                resultSet = ((PreparedStatement) statement).executeQuery();
                                                if (resultSet != null) {
                                                    if (testOnly) {
                                                        SqlUtils.testResultSet(resultSet);
                                                        throw new PetlException("Failed to load to SQL server due to testOnly mode");
                                                    } else {
                                                        log.trace("Setting up bulk copy connection");
                                                        Connection sqlServerConnection = getAsSqlServerConnection(targetConnection);
                                                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection);
                                                        SQLServerBulkCopyOptions bco = new SQLServerBulkCopyOptions();
                                                        bco.setKeepIdentity(true);
                                                        bco.setBatchSize(batchSize);
                                                        bco.setBulkCopyTimeout(timeout);
                                                        bulkCopy.setBulkCopyOptions(bco);
                                                        bulkCopy.setDestinationTableName(tableToBulkInsertInto);
                                                        log.debug("Starting bulk copy into " + tableToBulkInsertInto + " (batch size: " + batchSize + ", timeout: " + timeout + "s)");
                                                        timer.start("bulk copy");
                                                        progressMonitor.setRowCounter(tableToBulkInsertInto, () -> approximateRowCount(targetDatasource, tableToBulkInsertInto));
                                                        try {
                                                            bulkCopy.writeToServer(resultSet);
                                                        }
                                                        finally {
                                                            progressMonitor.setRowCounter(null, null);
                                                        }
                                                        log.trace("Bulk copy operation completed successfully");
                                                    }
                                                } else {
                                                    throw new PetlException("Invalid SQL extraction, no result set found");
                                                }
                                            } finally {
                                                DbUtils.closeQuietly(resultSet);
                                            }
                                        }
                                        sw.stop();
                                        log.trace("Statement executed in: " + sw);
                                    } finally {
                                        DbUtils.closeQuietly(statement);
                                    }
                                }
                                log.debug("Import Completed Successfully");
                            } finally {
                                try {
                                    sourceConnection.rollback();
                                } catch (Exception e) {
                                    log.debug("An error occurred during source connection rollback", e);
                                }
                                try {
                                    sourceConnection.setAutoCommit(originalSourceAutoCommit);
                                } catch (Exception e) {
                                    log.debug("An error occurred setting the source connection autocommit", e);
                                }
                                try {
                                    targetConnection.setAutoCommit(originalTargetAutocommit);
                                } catch (Exception e) {
                                    log.debug("An error occurred setting the target connection autocommit", e);
                                }
                            }
                        }
                    }

                Integer rowsImported = null;
                if (usePartitioning) {
                    timer.start("finalize");
                    Integer rowsAfterImport = logNumberOfRows("After Bulk Import:", targetDatasource, tableToBulkInsertInto);
                    if (rowsAfterImport != null) {
                        rowsImported = rowsAfterImport - (rowsBeforeImport == null ? 0 : rowsBeforeImport);
                    }
                    timer.start("lock wait");
                    synchronized (tableMonitor) {
                        timer.start("finalize");
                        log.debug("Moving partition " + partitionValue + " from " + tableToBulkInsertInto + " to " + targetTable);
                        targetDatasource.executeUpdate(SqlUtils.createMovePartitionStatement(tableToBulkInsertInto, targetTable, partitionValue));
                        log.debug("Dropping table: " + tableToBulkInsertInto);
                        targetDatasource.dropTableIfExists(tableToBulkInsertInto);

                        if (newWatermark != null) {
                            log.debug("Updating watermark for " + targetTable + " partition " + partitionValue + " from " + previousWatermark + " to " + newWatermark);
                            log.trace(updateWatermarkStatement);
                            targetDatasource.executeUpdate(updateWatermarkStatement);
                        }
                    }
                }
                timer.stop();
                log.info(importSummary(source, targetTable, usePartitioning ? partitionValue : null, rowsImported, timer));
            }
        }
        catch (Exception e) {
            String phase = timer.getCurrentPhase();
            timer.stop();
            log.info("Import from " + source + " failed during " + phase + " [" + timer + "]");
            throw e;
        }
        finally {
            progressMonitor.close();
            DockerConnector.stopContainers(containersStarted);
        }
    }

    /**
     * @return a connection for the given connection.  This allows mocking to occur in unit tests as needed
     * @param connection the connection
     * @throws SQLException if an error occurs
     */
    public Connection getAsSqlServerConnection(Connection connection) throws SQLException {
        if (connection.isWrapperFor(ISQLServerConnection.class)) {
            if (!(connection instanceof ISQLServerConnection)) {
                log.trace("The passed connection is a wrapper for ISQLServerConnection, unwrapping it.");
                return connection.unwrap(ISQLServerConnection.class);
            }
        }
        return connection;
    }

    /**
     * This method is synchronized so that if multiple jobs run in parallel that all check to see if the table needs updating,
     * that only one thread detects the change and recreates the table, and other threads will not detect a change
     */
    private synchronized void dropAndRecreateIfSchemasDiffer(DataSource targetDatasource, String existingTable, String newSchemaTable, String newSchema) throws SQLException {
        log.debug("Checking for schema changes between " + existingTable + " and " + newSchemaTable);
        List<TableColumn> existingColumns = targetDatasource.getTableColumns(existingTable);
        List<TableColumn> newColumns = targetDatasource.getTableColumns(newSchemaTable);
        boolean schemaChanged = newColumns.size() != existingColumns.size();
        if (!schemaChanged) {
            existingColumns.removeAll(newColumns);
            schemaChanged = !existingColumns.isEmpty();
        }
        if (schemaChanged) {
            log.debug("Change detected.  Dropping " + existingTable);
            log.trace("Existing=" + existingColumns);
            log.trace("New=" + newColumns);
            targetDatasource.dropTableIfExists(existingTable);
            log.debug("Creating new table");
            targetDatasource.executeUpdate(newSchema);
        }
    }

    /**
     * @return the number of rows in the given table, or null if this could not be determined
     */
    private Integer logNumberOfRows(String messagePrefix, DataSource dataSource, String tableName) {
        try {
            Integer numRows = dataSource.querySingleValue("select count(*) from " + tableName, Integer.class);
            log.debug(messagePrefix + " " + tableName + " contains " + numRows + " rows");
            return numRows;
        }
        catch (Exception e) {
            log.debug("Unable to count rows in " + tableName + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * @return the approximate number of rows in the given table, from SQL Server metadata.  This does not take locks
     * on the table, so it can be queried while a bulk copy into the table is in progress.
     */
    private Long approximateRowCount(DataSource dataSource, String tableName) throws SQLException {
        String sql = "select sum(row_count) from sys.dm_db_partition_stats " +
                "where object_id = object_id('" + tableName.replace("'", "''") + "') and index_id in (0, 1)";
        return dataSource.querySingleValue(sql, Long.class);
    }

    static String importSummary(String source, String targetTable, String partitionValue, Integer rowsImported, PhaseTimer timer) {
        StringBuilder sb = new StringBuilder("Imported ");
        sb.append(rowsImported == null ? "data" : String.format("%,d rows", rowsImported));
        sb.append(" from ").append(source).append(" into ").append(targetTable);
        if (partitionValue != null) {
            sb.append(" (partition ").append(partitionValue).append(")");
        }
        sb.append(" in ").append(LogUtils.formatDuration(timer.getTotalMillis()));
        sb.append(" [").append(timer).append("]");
        return sb.toString();
    }
}
