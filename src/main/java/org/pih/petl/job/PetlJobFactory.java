package org.pih.petl.job;

import java.io.File;

import org.pih.petl.PetlException;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

/**
 * Encapsulates a runnable pipeline
 */
public class PetlJobFactory {

    /**
     * Instantiate a new ETL PetlJob from the given configuration file
     */
    public static PetlJob instantiate(EtlService etlService, JobConfigReader configReader, String configFilePath) {
        File jobFile = configReader.getConfigFile(configFilePath);
        JobConfig jobConfig = configReader.getEtlJobConfigFromFile(jobFile);

        if ("job-pipeline".equals(jobConfig.getType())) {
            return new RunMultipleJob(etlService, configReader, configFilePath);
        }
        else if ("sqlserver-bulk-import".equals(jobConfig.getType())) {
            return new SqlServerImportJob(etlService, configReader, configFilePath);
        }
        else if ("pentaho-job".equals(jobConfig.getType())) {
            return new PentahoJob(etlService, configReader, configFilePath);
        }
        else {
            throw new PetlException("Invalid job type of " + jobConfig.getType());
        }
    }
}
