package org.pih.petl.job.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlUtil;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.datasource.DatabaseUtil;
import org.pih.petl.job.datasource.EtlDataSource;
import org.pih.petl.job.datasource.SqlStatementParser;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a particular ETL job configuration
 */
public class SqlJob implements PetlJob {

    private static Log log = LogFactory.getLog(SqlJob.class);

    /**
     * Creates a new instance of the job
     */
    public SqlJob() {
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        context.setStatus("Executing SqlJob");
        ApplicationConfig appConfig = context.getApplicationConfig();
        JobConfiguration config = context.getJobConfig().getConfiguration();

        Map<String, String> jobConfigVars = new HashMap<>(config.getVariables());
        JsonNode variableNode = config.get("variables");
        if (variableNode != null) {
            jobConfigVars.putAll(PetlUtil.getJsonAsMap(variableNode));
        }
        String delimiter = config.getString("delimiter");

        EtlDataSource dataSource = appConfig.getEtlDataSource(config.getString("datasource"));
        for (String sqlFile : config.getStringList("scripts")) {
            context.setStatus("Executing Sql Script: " + sqlFile);
            ConfigFile sourceSqlFile = appConfig.getConfigFile(sqlFile);
            try (Connection targetConnection = DatabaseUtil.openConnection(dataSource)) {
                String sqlFileContents = sourceSqlFile.getContentsWithVariableReplacement(jobConfigVars);
                if (StringUtils.isEmpty(delimiter)) {
                    try (Statement statement = targetConnection.createStatement()) {
                        log.trace("Executing: " + sqlFileContents);
                        statement.execute(sqlFileContents);
                    }
                }
                else {
                    List<String> stmts = SqlStatementParser.parseSqlIntoStatements(sqlFileContents, delimiter);
                    log.trace("Parsed extract query into " + stmts.size() + " statements");
                    for (String sqlStatement : stmts) {
                        if (StringUtils.isNotEmpty(sqlStatement)) {
                            try (Statement statement = targetConnection.createStatement()) {
                                log.trace("Executing: " + sqlStatement);
                                statement.execute(sqlStatement);
                            }
                        }
                    }
                }
            }
        }
    }
}
