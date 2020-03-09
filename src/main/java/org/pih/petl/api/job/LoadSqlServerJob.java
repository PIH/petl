package org.pih.petl.api.job;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.config.ConfigLoader;
import org.pih.petl.api.config.EtlDataSource;
import org.pih.petl.api.config.EtlJobConfig;
import org.pih.petl.api.db.DbQuery;
import org.pih.petl.api.db.EtlConnectionManager;
import org.pih.petl.api.db.StatementParser;
import org.pih.petl.api.status.EtlStatusTable;

/**
 * Job that can load into SQL Server table
 */
public class LoadSqlServerJob implements EtlJob {

    private static Log log = LogFactory.getLog(LoadSqlServerJob.class);
    private static boolean refreshInProgress = false;

    private EtlJobConfig config;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;

    //***** CONSTRUCTORS AND PROPERTY ACCESS *****

    public LoadSqlServerJob(EtlJobConfig config) {
        this.config = config;
    }

    /**
     * @see EtlJob
     */
    @Override
    public void execute() {
        if (!refreshInProgress) {
            refreshInProgress = true;
            try {
                String sds = config.getString("extract", "datasource");
                EtlDataSource sourceDatasource = ConfigLoader.getConfigurationFromFile(sds, EtlDataSource.class);
                String sourceQueryFile = config.getString("extract", "query");
                String sourceQuery = ConfigLoader.getFileContents(sourceQueryFile);
                String targetTable = config.getString("load", "table");
                String targetSchemaFile = config.getString("load", "schema");
                String targetSchema = ConfigLoader.getFileContents(targetSchemaFile);
                String tds = config.getString("load", "datasource");
                EtlDataSource targetDatasource = ConfigLoader.getConfigurationFromFile(tds, EtlDataSource.class);

                // TODO: Add validation in

                // Initialize the status table with this job execution
                String uuid = EtlStatusTable.createStatus(targetTable);

                try {
                    QueryRunner qr = new QueryRunner();
                    sourceConnection = EtlConnectionManager.openConnection(sourceDatasource);
                    targetConnection = EtlConnectionManager.openConnection(targetDatasource);

                    boolean originalSourceAutoCommit = sourceConnection.getAutoCommit();
                    boolean originalTargetAutocommit = targetConnection.getAutoCommit();

                    RowCountUpdater updater = new RowCountUpdater(targetConnection, uuid, targetTable);
                    try {
                        updater.start();
                        sourceConnection.setAutoCommit(false); // We intend to rollback changes to source after querying DB
                        targetConnection.setAutoCommit(true);  // We want to commit to target as we go, to query status

                        // First, drop any existing target table
                        EtlStatusTable.updateStatus(uuid, "Dropping existing table");
                        qr.update(targetConnection, "drop table if exists " + targetTable);

                        // Then, recreate the target table
                        EtlStatusTable.updateStatus(uuid, "Creating table");
                        qr.update(targetConnection, targetSchema);

                        // Now execute a bulk import
                        EtlStatusTable.updateStatus(uuid, "Executing import");

                        // Parse the source query into statements
                        List<String> stmts = StatementParser.parseSqlIntoStatements(sourceQuery, ";");
                        log.debug("Parsed extract query into " + stmts.size() + " statements");

                        // Iterate over each statement, and execute.  The final statement is expected to select the data out.
                        for (Iterator<String> sqlIterator = stmts.iterator(); sqlIterator.hasNext();) {
                            String sqlStatement = sqlIterator.next();
                            Statement statement = null;
                            try {
                                log.debug("Executing: " + sqlStatement);
                                StopWatch sw = new StopWatch();
                                sw.start();
                                statement = sourceConnection.createStatement();
                                statement.execute(sqlStatement);
                                log.debug("Statement executed");
                                if (!sqlIterator.hasNext()) {
                                    log.debug("This is the last statement, treat it as the extraction query");
                                    ResultSet resultSet = null;
                                    try {
                                        resultSet = statement.getResultSet();
                                        if (resultSet != null) {
                                            // Skip to the end to get the number of rows that ResultSet contains
                                            resultSet.last();
                                            Integer rowCount = resultSet.getRow();
                                            EtlStatusTable.updateTotalCount(uuid, rowCount);

                                            // Reset back to the beginning to ensure all rows are extracted
                                            resultSet.beforeFirst();

                                            // Pass the ResultSet to bulk copy to SQL Server (TODO: Handle other DBs)
                                            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(targetConnection);
                                            SQLServerBulkCopyOptions bco = new SQLServerBulkCopyOptions();
                                            bco.setKeepIdentity(true);
                                            bco.setBatchSize(100);
                                            bco.setBulkCopyTimeout(3600);
                                            bulkCopy.setBulkCopyOptions(bco);
                                            bulkCopy.setDestinationTableName(targetTable);
                                            bulkCopy.writeToServer(resultSet);
                                        }
                                        else {
                                            throw new IllegalStateException("Invalid SQL extraction, no result set found");
                                        }
                                    }
                                    finally {
                                        DbUtils.closeQuietly(resultSet);
                                    }
                                }
                                sw.stop();
                                log.debug("Statement executed in: " + sw.toString());
                            }
                            finally {
                                DbUtils.closeQuietly(statement);
                            }
                        }

                        // Update the status at the end of the bulk copy
                        EtlStatusTable.updateCurrentCount(uuid, DbQuery.rowCount(targetConnection, targetTable));
                        EtlStatusTable.updateStatusSuccess(uuid);
                    }
                    catch (Exception e) {
                        EtlStatusTable.updateStatusError(uuid, e);
                    }
                    finally {
                        updater.cancel();
                        sourceConnection.rollback();
                        sourceConnection.setAutoCommit(originalSourceAutoCommit);
                        targetConnection.setAutoCommit(originalTargetAutocommit);
                    }
                }
                finally {
                    DbUtils.closeQuietly(targetConnection);
                    DbUtils.closeQuietly(sourceConnection);
                }
            }
            catch (Exception e) {
                throw new IllegalStateException("An error occured during SQL Server Load task", e);
            }
            finally {
                refreshInProgress = false;
            }
        }
    }

    class RowCountUpdater extends Thread {

        private long lastExecutionTime = System.currentTimeMillis();
        private long msBetweenExecutions = 1000*5;

        private Connection connection;
        private String uuid;
        private String table;

        public RowCountUpdater(Connection connection, String uuid, String table) {
            this.connection = connection;
            this.uuid = uuid;
            this.table = table;
        }

        @Override
        public void run() {
            while (connection != null) {
                long msSinceLast = System.currentTimeMillis() - lastExecutionTime;
                if (msSinceLast >= msBetweenExecutions) {
                    try {
                        Integer rowCount = DbQuery.rowCount(connection, table);
                        EtlStatusTable.updateCurrentCount(uuid, rowCount);
                    }
                    catch (Exception e) {}
                    lastExecutionTime = System.currentTimeMillis();
                }
            }
        }

        public void cancel() {
            connection = null;
        }
    }
}
