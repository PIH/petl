package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * PetlJob that can stream rows from a source SQL DB and load these in bulk into a target DB
 */
@Component("sql-transfer")
public class SqlTransferJob implements PetlJob {

    private final Log log = LogFactory.getLog(getClass());

    private final Map<String, Object> tableMonitors = new HashMap<>();

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {

        log.debug("Executing " + getClass().getSimpleName());
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        List<String> containersStarted = new ArrayList<>();
        try {
            // Get source datasource
            DataSource sourceDatasource = configReader.getDataSource("extract", "datasource");
            if (sourceDatasource.startContainerIfNecessary()) {
                containersStarted.add(sourceDatasource.getContainerName());
            }
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
            if (targetDatasource.startContainerIfNecessary()) {
                containersStarted.add(targetDatasource.getContainerName());
            }
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
                } else {
                    targetSchema = SqlUtils.addExtraColumnsToSchema(targetSchema, extraColumns);
                }
            }

            boolean dropAndRecreate = configReader.getBoolean(true, "load", "dropAndRecreateTable");

            Object tableMonitor = tableMonitors.computeIfAbsent(targetTable, k -> new Object());
            synchronized (tableMonitor) {

                if (StringUtils.isNotEmpty(targetSchema)) {
                    if (dropAndRecreate) {
                        log.debug("Dropping existing table: " + targetTable);
                        targetDatasource.dropTableIfExists(targetTable);
                    }
                    if (!targetDatasource.tableExists(targetTable)) {
                        log.debug("Creating target schema for: " + targetTable);
                        targetDatasource.executeUpdate(targetSchema);
                    } else {
                        log.debug("Target table already exists at: " + targetTable);
                    }
                } else {
                    log.debug("No target schema specified");
                }

                // Get bulk load configuration
                int batchSize = configReader.getInt(100, "load", "bulkCopy", "batchSize");
                boolean testOnly = configReader.getBoolean(false, "load", "bulkCopy", "testOnly");

                try (Connection sourceConnection = sourceDatasource.openConnection()) {
                    try (Connection targetConnection = targetDatasource.openConnection()) {

                        boolean originalSourceAutoCommit = sourceConnection.getAutoCommit();
                        boolean originalTargetAutocommit = targetConnection.getAutoCommit();

                        try {
                            sourceConnection.setAutoCommit(false); // We intend to rollback changes to source after querying DB
                            targetConnection.setAutoCommit(false);  // We will commit after each batch

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
                                        PreparedStatement targetStatement = null;
                                        try {
                                            resultSet = ((PreparedStatement) statement).executeQuery();
                                            if (resultSet != null) {
                                                if (testOnly) {
                                                    SqlUtils.testResultSet(resultSet);
                                                    throw new PetlException("Failed to load to SQL server due to testOnly mode");
                                                } else {
                                                    ResultSetMetaData metaData = resultSet.getMetaData();
                                                    int numPending = 0;
                                                    int numCompleted = 0;
                                                    while (resultSet.next()) {
                                                        if (targetStatement == null) {
                                                            StringBuilder targetColumns = new StringBuilder();
                                                            StringBuilder targetValues = new StringBuilder();
                                                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                                                targetColumns.append(targetColumns.length() == 0 ? "" : ",").append(metaData.getColumnName(i));
                                                                targetValues.append(targetValues.length() == 0 ? "" : ",").append("?");
                                                            }
                                                            targetStatement = targetConnection.prepareStatement("insert into " + targetTable + " (" + targetColumns + ") values (" + targetValues + ");");
                                                        }

                                                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                                            targetStatement.setObject(i, resultSet.getObject(i));
                                                        }
                                                        targetStatement.addBatch();
                                                        numPending++;
                                                        if (numPending % batchSize == 0) {
                                                            log.trace("Executing batch of size " + numPending + " -> " + targetTable);
                                                            targetStatement.executeBatch();
                                                            targetConnection.commit();
                                                            numCompleted += numPending;
                                                            numPending = 0;
                                                        }
                                                    }
                                                    if (numPending > 0) {
                                                        log.trace("Executing final batch of size " + numPending + " -> " + targetTable);
                                                        targetStatement.executeBatch();
                                                        targetConnection.commit();
                                                        numCompleted += numPending;
                                                    }
                                                    log.trace("Successfully transferred " + numCompleted + " rows into table: " + targetTable);
                                                }
                                            } else {
                                                throw new PetlException("Invalid SQL extraction, no result set found");
                                            }
                                        } catch (Exception e) {
                                            log.error("An error occurred during bulk copy operation", e);
                                            throw e;
                                        } finally {
                                            DbUtils.closeQuietly(resultSet);
                                            DbUtils.closeQuietly(targetStatement);
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
            }
        }
        finally {
            DockerConnector.stopContainers(containersStarted);
        }
    }
}
