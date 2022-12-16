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
import org.pih.petl.PetlException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * PetlJob that can load into SQL Server table
 */
@Component("sqlserver-bulk-import")
public class SqlServerImportJob extends AbstractJob {

    private final Log log = LogFactory.getLog(getClass());

    private final Map<String, Object> tableMonitors = new HashMap<>();

    @Autowired
    public SqlServerImportJob(ApplicationConfig applicationConfig) {
        super(applicationConfig);
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {

        log.debug("Executing SqlServerImportJob");
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        // Get source datasource
        DataSource sourceDatasource = configReader.getDataSource("extract", "datasource");
        if (!sourceDatasource.testConnection()) {
            String msg = "Unable to connect to datasource: " + configReader.getString("extract", "datasource");
            log.debug(msg);
            throw new PetlException(msg);
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
        if (!targetDatasource.testConnection()) {
            String msg = "Unable to connect to datasource: " + configReader.getString("load", "datasource");
            log.debug(msg);
            throw new PetlException(msg);
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
            }
            else {
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
            }
            else if (StringUtils.isEmpty(partitionScheme)) {
                throw new PetlException("You must specify a partition scheme if you specify a partition column");
            }
            else if (StringUtils.isEmpty(partitionColumn)) {
                throw new PetlException("You must specify a partition column if you specify a partition scheme");
            }
            else {
                targetSchema = SqlUtils.addPartitionSchemeToSchema(targetSchema, partitionScheme, partitionColumn);
                usePartitioning = true;
            }
        }
        if (usePartitioning && StringUtils.isEmpty(partitionValue)) {
            throw new PetlException("You must specify a value for your partition column");
        }

        Object tableMonitor = tableMonitors.computeIfAbsent(targetTable, k -> new Object());
        synchronized (tableMonitor) {

            String tableToBulkInsertInto = targetTable;

            if (usePartitioning) {
                tableToBulkInsertInto = targetTable + "_" + partitionValue;
                targetDatasource.dropTableIfExists(tableToBulkInsertInto);
                String partitionSchema = SqlUtils.addSuffixToCreatedTablename(targetSchema, "_" + partitionValue);
                targetDatasource.executeUpdate(partitionSchema);
                dropAndRecreateIfSchemasDiffer(targetDatasource, targetTable, tableToBulkInsertInto, targetSchema);
            } else {
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
                                    ResultSet resultSet = null;
                                    try {
                                        resultSet = ((PreparedStatement) statement).executeQuery();
                                        if (resultSet != null) {
                                            Connection sqlServerConnection = getAsSqlServerConnection(targetConnection);
                                            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection);
                                            SQLServerBulkCopyOptions bco = new SQLServerBulkCopyOptions();
                                            bco.setKeepIdentity(true);
                                            bco.setBatchSize(100);
                                            bco.setBulkCopyTimeout(3600);
                                            bulkCopy.setBulkCopyOptions(bco);
                                            bulkCopy.setDestinationTableName(tableToBulkInsertInto);
                                            bulkCopy.writeToServer(resultSet);
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
                        log.debug("Import Completed Sucessfully");
                    } finally {
                        sourceConnection.rollback();
                        sourceConnection.setAutoCommit(originalSourceAutoCommit);
                        targetConnection.setAutoCommit(originalTargetAutocommit);
                    }
                }
            }

            if (usePartitioning) {
                targetDatasource.executeUpdate(SqlUtils.createMovePartitionStatement(tableToBulkInsertInto, targetTable, partitionValue));
                targetDatasource.dropTableIfExists(tableToBulkInsertInto);
            }
        }
    }

    /**
     * @return a connection for the given connection.  This allows mocking to occur in unit tests as needed
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
}
