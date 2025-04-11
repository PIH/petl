package org.pih.petl.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.DockerConnector;
import org.pih.petl.PetlException;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.config.TableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * PetlJob that can create a table
 */
@Component("create-table")
public class CreateTableJob implements PetlJob {

    private final Log log = LogFactory.getLog(getClass());

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {
        log.debug("Executing CreateTableJob");
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        List<String> containersStarted = new ArrayList<>();
        try {
            // Get source datasource
            DataSource sourceDatasource = configReader.getDataSource("source", "datasource");
            if (sourceDatasource != null && sourceDatasource.startContainerIfNecessary()) {
                containersStarted.add(sourceDatasource.getContainerName());
            }
            String sourceTable = configReader.getString("source", "tableName");
            String sourceSql = configReader.getFileContents("source", "sqlFile");

            // Get target datasource
            DataSource targetDatasource = configReader.getDataSource("target", "datasource");
            if (targetDatasource != null && targetDatasource.startContainerIfNecessary()) {
                containersStarted.add(targetDatasource.getContainerName());
            }
            String targetTable = configReader.getString("target", "tableName");
            String actionIfExists = configReader.getString("target", "actionIfExists");

            // Check if table already exists

            boolean tableExists = targetDatasource.tableExists(targetTable);
            if (tableExists) {
                log.debug("Table " + targetTable + " already exists.");
                if ("drop".equalsIgnoreCase(actionIfExists)) {
                    log.debug("Dropping existing table");
                    targetDatasource.dropTableIfExists(targetTable);
                    tableExists = false;
                } else if ("dropIfChanged".equalsIgnoreCase(actionIfExists)) {
                    log.debug("Checking whether target table schema has changed");
                    List<TableColumn> existingColumns = targetDatasource.getTableColumns(targetTable);
                    List<TableColumn> newColumns;
                    if (sourceDatasource != null && StringUtils.isNotEmpty(sourceTable)) {
                        log.debug("Checking source datasource for column definitions");
                        newColumns = sourceDatasource.getTableColumns(sourceTable);
                    } else if (StringUtils.isNotEmpty(sourceSql)) {
                        log.debug("Checking target sql for schema changes, and dropping if schema has changed");
                        String tempTableName = SqlUtils.getTableName(sourceSql) + "_temp";
                        String tempTableSchema = SqlUtils.addSuffixToCreatedTablename(sourceSql, "_temp");
                        targetDatasource.dropTableIfExists(tempTableName);
                        targetDatasource.executeUpdate(tempTableSchema);
                        newColumns = targetDatasource.getTableColumns(tempTableName);
                        targetDatasource.dropTableIfExists(tempTableName);
                    } else {
                        throw new PetlException("You must provide either a source datasource and tableName or a source sqlFile");
                    }
                    boolean schemaChanged = newColumns.size() != existingColumns.size();
                    if (!schemaChanged) {
                        existingColumns.removeAll(newColumns);
                        schemaChanged = !existingColumns.isEmpty();
                    }
                    if (schemaChanged) {
                        log.debug("Change detected.  Dropping existing schema");
                        log.trace("Existing=" + existingColumns);
                        log.trace("New=" + newColumns);
                        targetDatasource.dropTableIfExists(targetTable);
                        tableExists = false;
                    }
                }
            }

            if (tableExists) {
                log.debug("Table " + targetTable + " already exists.  Not recreating");
                return;
            }

            String schemaToExecute = sourceSql;

            // Load in table schema from the provided data source and table
            if (sourceDatasource != null && StringUtils.isNotEmpty(sourceTable)) {
                if (schemaToExecute != null) {
                    throw new PetlException("You cannot specify both source sql and source datasource and table");
                }
                StringBuilder stmt = new StringBuilder();
                for (TableColumn column : sourceDatasource.getTableColumns(sourceTable)) {
                    if (stmt.length() == 0) {
                        stmt.append("create table ").append(targetTable).append(" (");
                    } else {
                        stmt.append(", ");
                    }
                    stmt.append(System.lineSeparator()).append(column.getName()).append(" ").append(column.getType());
                }
                stmt.append(")");
                schemaToExecute = stmt.toString();
            }

            if (schemaToExecute == null) {
                throw new PetlException("No schema to execute was found");
            }

            log.debug("Creating schema");
            log.trace(schemaToExecute);
            targetDatasource.executeUpdate(schemaToExecute);
        }
        finally {
            DockerConnector.stopContainers(containersStarted);
        }
    }

}
