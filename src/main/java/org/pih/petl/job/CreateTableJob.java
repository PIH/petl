package org.pih.petl.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.config.TableColumn;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * PetlJob that can create a table
 */
@Component("create-table")
public class CreateTableJob implements PetlJob {

    private static final Log log = LogFactory.getLog(CreateTableJob.class);

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        context.setStatus("Executing CreateTableJob");
        JobConfigReader configReader = new JobConfigReader(context);

        // Get source datasource
        DataSource sourceDatasource = configReader.getDataSource("source", "datasource");
        String sourceTable = configReader.getString("source", "tableName");
        String sourceSql = configReader.getFileContents("source", "sqlFile");

        // Get target datasource
        DataSource targetDatasource = configReader.getDataSource("target", "datasource");
        String targetTable = configReader.getString("target", "tableName");
        String actionIfExists = configReader.getString("target", "actionIfExists");

        // Check if table already exists

        boolean tableExists = targetDatasource.tableExists(targetTable);
        if (tableExists) {
            context.setStatus("Table " + targetTable + " already exists.");
            if ("drop".equalsIgnoreCase(actionIfExists)) {
                context.setStatus("Dropping existing table");
                targetDatasource.dropTableIfExists(targetTable);
                tableExists = false;
            }
            else if ("dropIfChanged".equalsIgnoreCase(actionIfExists)) {
                context.setStatus("Checking whether target table schema has changed");
                List<TableColumn> existingColumns = targetDatasource.getTableColumns(targetTable);
                List<TableColumn> newColumns;
                if (sourceDatasource != null && StringUtils.isNotEmpty(sourceTable)) {
                    context.setStatus("Checking source datasource for column definitions");
                    newColumns = sourceDatasource.getTableColumns(sourceTable);
                }
                else if (StringUtils.isNotEmpty(sourceSql)) {
                    context.setStatus("Checking target sql for schema changes, and dropping if schema has changed");
                    String tempTableName = SqlUtils.getTableName(sourceSql) + "_temp";
                    String tempTableSchema = SqlUtils.addSuffixToCreatedTablename(sourceSql, "_temp");
                    targetDatasource.dropTableIfExists(tempTableName);
                    targetDatasource.executeUpdate(tempTableSchema);
                    newColumns = targetDatasource.getTableColumns(tempTableName);
                    targetDatasource.dropTableIfExists(tempTableName);
                }
                else {
                    throw new PetlException("You must provide either a source datasource and tableName or a source sqlFile");
                }
                boolean schemaChanged = newColumns.size() != existingColumns.size();
                if (!schemaChanged) {
                    existingColumns.removeAll(newColumns);
                    schemaChanged = !existingColumns.isEmpty();
                }
                if (schemaChanged) {
                    context.setStatus("Change detected.  Dropping existing schema");
                    log.trace("Existing=" + existingColumns);
                    log.trace("New=" + newColumns);
                    targetDatasource.dropTableIfExists(targetTable);
                    tableExists = false;
                }
            }
        }

        if (tableExists) {
            context.setStatus("Table " + targetTable + " already exists.  Not recreating");
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
                }
                else {
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

        context.setStatus("Creating schema");
        log.trace(schemaToExecute);
        targetDatasource.executeUpdate(schemaToExecute);
    }

}
