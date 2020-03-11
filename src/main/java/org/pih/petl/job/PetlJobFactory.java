package org.pih.petl.job;

import org.pih.petl.PetlException;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.ConfigFileReader;

/**
 * Encapsulates a runnable pipeline
 */
public class PetlJobFactory {

    /**
     * Instantiate a new ETL PetlJob from the given configuration file
     */
    public static PetlJob instantiate(EtlService etlService, String configFilePath) {
        ConfigFile jobFile = etlService.getConfigFileReader().getConfigFile(configFilePath);
        JobConfig jobConfig = etlService.getConfigFileReader().read(jobFile, JobConfig.class);

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
