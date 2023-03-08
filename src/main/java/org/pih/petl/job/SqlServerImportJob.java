package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PetlJob that can load into SQL Server table
 */
@Component("sqlserver-bulk-import")
public class SqlServerImportJob implements PetlJob {

    private final Log log = LogFactory.getLog(getClass());

    private final Map<String, Object> tableMonitors = new HashMap<>();

    @Autowired
    ApplicationConfig applicationConfig;

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

            // Get bulk load configuration
            int batchSize = configReader.getInt(100, "load", "bulkCopy", "batchSize");
            int timeout = configReader.getInt(7200, "load", "bulkCopy", "timeout"); // 2h default
            SqlUtils.bulkLoadIntoSqlServer(sourceDatasource, targetDatasource, sourceQuery, extraColumns, batchSize, timeout, tableToBulkInsertInto);

            if (usePartitioning) {
                targetDatasource.executeUpdate(SqlUtils.createMovePartitionStatement(tableToBulkInsertInto, targetTable, partitionValue));
                targetDatasource.dropTableIfExists(tableToBulkInsertInto);
            }
        }
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
