package org.pih.petl.job;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.EtlStatus;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;

/**
 * Encapsulates a particular ETL job configuration
 */
public class RunMultipleJob implements PetlJob {

    private static Log log = LogFactory.getLog(RunMultipleJob.class);
    private static boolean refreshInProgress = false;

    private EtlService etlService;
    private String configPath;

    /**
     * Creates a new instance of the job with the given configuration path
     */
    public RunMultipleJob(EtlService etlService, String configPath) {
        this.etlService = etlService;
        this.configPath = configPath;
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute() {
        if (!refreshInProgress) {
            refreshInProgress = true;
            try {
                PetlJobConfig config = etlService.loadJobConfig(configPath);
                String jobName = configPath;
                List<String> jobs = config.getStringList("jobs");
                boolean parallelExecution = config.getBoolean("parallelExecution");

                EtlStatus etlStatus = etlService.createStatus(jobName);
                etlStatus.setTotalExpected(jobs.size());
                etlService.updateEtlStatus(etlStatus);

                // TODO: Handle the ability to utilize the parallel execution and run jobs in parallel to each other
                for (String jobPath : jobs) {
                    etlStatus.setStatus("Running job: " + jobPath);
                    etlService.updateEtlStatus(etlStatus);
                    PetlJob job = PetlJobFactory.instantiate(etlService, jobPath);
                    job.execute();
                }
            }
            finally {
                refreshInProgress = false;
            }
        }
    }
}
