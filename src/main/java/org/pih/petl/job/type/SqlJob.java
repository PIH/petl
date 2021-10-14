package org.pih.petl.job.type;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.datasource.DatabaseUtil;
import org.pih.petl.job.datasource.EtlDataSource;

import java.sql.Connection;

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

        EtlDataSource dataSource = appConfig.getEtlDataSource(config.getString("datasource"));
        for (String sqlFile : config.getStringList("scripts")) {
            context.setStatus("Executing Sql Script: " + sqlFile);
            ConfigFile sourceSqlFile = appConfig.getConfigFile(sqlFile);
            String sqlToExecute = sourceSqlFile.getContentsWithVariableReplacement(config.getVariables());
            try (Connection targetConnection = DatabaseUtil.openConnection(dataSource)) {
                QueryRunner qr = new QueryRunner();
                log.debug("Executing: " + sqlToExecute);
                qr.update(targetConnection, sqlToExecute);
            }
        }
    }
}
