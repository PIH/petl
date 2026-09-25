package org.pih.petl.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.DockerConnector;
import org.pih.petl.LogUtils;
import org.pih.petl.PetlException;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates a particular ETL job configuration
 */
@Component("sql-execution")
public class SqlJob implements PetlJob {

    private static final long SLOW_STATEMENT_MILLIS = 60000;

    private final Log log = LogFactory.getLog(getClass());

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {
        log.debug("Executing SqlJob");
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());

        String delimiter = configReader.getString("delimiter");

        DataSource dataSource = configReader.getDataSource("datasource");
        boolean containerStarted = dataSource.startContainerIfNecessary();
        try {
            for (String sqlFile : configReader.getStringList("scripts")) {
                log.debug("Executing Sql Script: " + sqlFile);
                long scriptStart = System.currentTimeMillis();
                int numStatements;
                try (Connection targetConnection = dataSource.openConnection()) {
                    String sqlFileContents = configReader.getFileContentsAtPath(sqlFile);
                    List<String> stmts;
                    if (StringUtils.isEmpty(delimiter)) {
                        stmts = Collections.singletonList(sqlFileContents);
                    } else {
                        stmts = SqlUtils.parseSqlIntoStatements(sqlFileContents, delimiter);
                        log.trace("Parsed extract query into " + stmts.size() + " statements");
                    }
                    numStatements = 0;
                    for (int i = 0; i < stmts.size(); i++) {
                        String sqlStatement = stmts.get(i);
                        if (StringUtils.isNotEmpty(sqlStatement)) {
                            numStatements++;
                            executeStatement(targetConnection, sqlFile, sqlStatement, i + 1, stmts.size());
                        }
                    }
                }
                long duration = System.currentTimeMillis() - scriptStart;
                log.info("Executed " + sqlFile + " in " + LogUtils.formatDuration(duration) + " (" + numStatements + (numStatements == 1 ? " statement)" : " statements)"));
            }
        }
        finally {
            if (containerStarted) {
                DockerConnector.stopContainer(dataSource.getContainerName());
            }
        }
    }

    private void executeStatement(Connection connection, String sqlFile, String sqlStatement, int statementNum, int numStatements) {
        String statementDescription = (numStatements > 1 ? "statement " + statementNum + " of " + numStatements + " in " : "") + sqlFile;
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            log.trace("Executing: " + sqlStatement);
            statement.execute(sqlStatement);
        }
        catch (Exception e) {
            log.debug("Failed SQL: " + sqlStatement);
            throw new PetlException("Error in " + statementDescription + ": " + LogUtils.abbreviateSql(sqlStatement, 200), e);
        }
        long duration = System.currentTimeMillis() - start;
        if (duration >= SLOW_STATEMENT_MILLIS) {
            log.info("Slow statement: " + statementDescription + " took " + LogUtils.formatDuration(duration) + ": " + LogUtils.abbreviateSql(sqlStatement, 100));
        }
    }
}
