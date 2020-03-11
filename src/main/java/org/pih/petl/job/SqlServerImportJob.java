package org.pih.petl.job;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.EtlStatus;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.datasource.EtlConnectionManager;
import org.pih.petl.job.datasource.EtlDataSource;
import org.pih.petl.job.datasource.SqlStatementParser;
import org.pih.petl.job.datasource.SqlUtils;

/**
 * PetlJob that can load into SQL Server table
 */
public class SqlServerImportJob implements PetlJob {

    private static Log log = LogFactory.getLog(SqlServerImportJob.class);
    private static boolean refreshInProgress = false;

    private EtlService etlService;
    private JobConfigReader configReader;
    private ConfigFile configFile;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;

    /**
     * Creates a new instance of the job with the given configuration path
     */
    public SqlServerImportJob(EtlService etlService, JobConfigReader configReader, String configPath) {
        this.etlService = etlService;
        this.configReader = configReader;
        this.configFile = configReader.getConfigFile(configPath);
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute() {
        if (!refreshInProgress) {
            refreshInProgress = true;
            try {
                JobConfig config = configReader.read(configFile, JobConfig.class);
                String jobName = configFile.getFilePath();

                // Get source datasource
                String sourceDataSourceFilename = config.getString("extract", "datasource");
                ConfigFile sourceDataFile = configReader.getConfigFile(sourceDataSourceFilename);
                EtlDataSource sourceDatasource = configReader.read(sourceDataFile, EtlDataSource.class);

                // Get source query
                String sourceQueryFileName = config.getString("extract", "query");
                ConfigFile sourceQueryFile = configReader.getConfigFile(sourceQueryFileName);
                String sourceQuery = sourceQueryFile.getContents();

                // Get target datasource
                String targetDataFileName = config.getString("load", "datasource");
                ConfigFile targetDataFile = configReader.getConfigFile(targetDataFileName);
                EtlDataSource targetDatasource = configReader.read(targetDataFile, EtlDataSource.class);

                // Get target table name
                String targetTable = config.getString("load", "table");

                // Get target table schema
                String targetSchemaFilename = config.getString("load", "schema");
                ConfigFile targetSchemaFile = configReader.getConfigFile(targetSchemaFilename);
                String targetSchema = targetSchemaFile.getContents();

                // TODO: Add validation in

                // Initialize the status table with this job execution
                EtlStatus etlStatus = etlService.createStatus(jobName);

                try {
                    QueryRunner qr = new QueryRunner();
                    sourceConnection = EtlConnectionManager.openConnection(sourceDatasource);
                    targetConnection = EtlConnectionManager.openConnection(targetDatasource);

                    boolean originalSourceAutoCommit = sourceConnection.getAutoCommit();
                    boolean originalTargetAutocommit = targetConnection.getAutoCommit();

                    RowCountUpdater updater = new RowCountUpdater(targetConnection, etlStatus, targetTable);
                    try {
                        updater.start();
                        sourceConnection.setAutoCommit(false); // We intend to rollback changes to source after querying DB
                        targetConnection.setAutoCommit(true);  // We want to commit to target as we go, to query status

                        // First, drop any existing target table
                        etlStatus.setStatus("Dropping existing table");
                        etlService.updateEtlStatus(etlStatus);
                        qr.update(targetConnection, "drop table if exists " + targetTable);

                        // Then, recreate the target table
                        etlStatus.setStatus("Creating table");
                        etlService.updateEtlStatus(etlStatus);
                        qr.update(targetConnection, targetSchema);

                        // Now execute a bulk import
                        etlStatus.setStatus("Executing import");
                        etlService.updateEtlStatus(etlStatus);

                        // Parse the source query into statements
                        List<String> stmts = SqlStatementParser.parseSqlIntoStatements(sourceQuery, ";");
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
                                            etlStatus.setTotalExpected(rowCount);
                                            etlService.updateEtlStatus(etlStatus);

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
                                            throw new PetlException("Invalid SQL extraction, no result set found");
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
                        Integer rowCount = SqlUtils.rowCount(targetConnection, targetTable);
                        etlStatus.setTotalLoaded(rowCount);
                        etlStatus.setStatus("Import Completed Sucessfully");
                        etlStatus.setCompleted(new Date());
                        etlService.updateEtlStatus(etlStatus);
                    }
                    catch (Exception e) {
                        etlStatus.setStatus("Import Failed");
                        etlStatus.setCompleted(new Date());
                        etlStatus.setErrorMessage(e.getMessage());
                        etlService.updateEtlStatus(etlStatus);
                        log.error("PETL Job Failed", e);
                        throw e;
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
                throw new PetlException("An error occured during SQL Server Load task", e);
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
        private EtlStatus status;
        private String table;

        public RowCountUpdater(Connection connection, EtlStatus status, String table) {
            this.connection = connection;
            this.status = status;
            this.table = table;
        }

        @Override
        public void run() {
            while (connection != null) {
                long msSinceLast = System.currentTimeMillis() - lastExecutionTime;
                if (msSinceLast >= msBetweenExecutions) {
                    try {
                        Integer rowCount = SqlUtils.rowCount(connection, table);
                        status.setTotalLoaded(rowCount);
                        etlService.updateEtlStatus(status);
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
