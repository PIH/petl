package org.pih.petl.job.config;

import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.PentahoJob;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.RunMultipleJob;
import org.pih.petl.job.SqlServerImportJob;

/**
 * Encapsulates a runnable pipeline
 */
public class PetlJobFactory {

    /**
     * Instantiate a new ETL PetlJob from the given configuration file
     */
    public static PetlJob instantiate(EtlService etlService, String configFilePath) {

        ApplicationConfig config = etlService.getApplicationConfig();
        ConfigFile jobFile = config.getConfigFile(configFilePath);
        PetlJobConfig jobConfig = config.loadConfiguration(jobFile, PetlJobConfig.class);

        if ("job-pipeline".equals(jobConfig.getType())) {
            return new RunMultipleJob(etlService, configFilePath);
        }
        else if ("sqlserver-bulk-import".equals(jobConfig.getType())) {
            return new SqlServerImportJob(etlService, configFilePath);
        }
        else if ("pentaho-job".equals(jobConfig.getType())) {
            return new PentahoJob(etlService, configFilePath);
        }
        else {
            throw new PetlException("Invalid job type of " + jobConfig.getType());
        }
    }
}
