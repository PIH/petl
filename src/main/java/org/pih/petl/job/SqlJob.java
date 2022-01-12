package org.pih.petl.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * Encapsulates a particular ETL job configuration
 */
@Component("sql-execution")
public class SqlJob implements PetlJob {

    private static Log log = LogFactory.getLog(SqlJob.class);

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        context.setStatus("Executing SqlJob");
        JobConfigReader configReader = new JobConfigReader(context);

        String delimiter = configReader.getString("delimiter");

        DataSource dataSource = configReader.getDataSource("datasource");
        for (String sqlFile : configReader.getStringList("scripts")) {
            context.setStatus("Executing Sql Script: " + sqlFile);
            try (Connection targetConnection = dataSource.openConnection()) {
                String sqlFileContents = configReader.getFileContentsAtPath(sqlFile);
                if (StringUtils.isEmpty(delimiter)) {
                    try (Statement statement = targetConnection.createStatement()) {
                        log.trace("Executing: " + sqlFileContents);
                        statement.execute(sqlFileContents);
                    }
                    catch(Exception e) {
                        throw new PetlException("Error executing statement: " + sqlFileContents, e);
                    }
                }
                else {
                    List<String> stmts = SqlUtils.parseSqlIntoStatements(sqlFileContents, delimiter);
                    log.trace("Parsed extract query into " + stmts.size() + " statements");
                    for (String sqlStatement : stmts) {
                        if (StringUtils.isNotEmpty(sqlStatement)) {
                            try (Statement statement = targetConnection.createStatement()) {
                                log.trace("Executing: " + sqlStatement);
                                statement.execute(sqlStatement);
                            }
                            catch(Exception e) {
                                throw new PetlException("Error executing statement: " + sqlStatement, e);
                            }
                        }
                    }
                }
            }
        }
    }
}
