package org.pih.petl.api.job;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.EtlJobRunner;
import org.pih.petl.api.config.EtlJobConfig;

/**
 * Encapsulates a particular ETL job configuration
 */
public class RunMultipleJob implements EtlJob {

    private static Log log = LogFactory.getLog(RunMultipleJob.class);
    private static boolean refreshInProgress = false;

    private EtlJobConfig config;

    //***** CONSTRUCTORS *****

    public RunMultipleJob(EtlJobConfig config) {
        this.config = config;
    }

    /**
     * @see EtlJob
     */
    @Override
    public void execute() throws Exception {
        if (!refreshInProgress) {
            refreshInProgress = true;
            try {
                // Pull options off of configuration
                List<String> jobs = config.getStringList("jobs");
                boolean parallelExecution = config.getBoolean("parallelExecution");

                for (String job : jobs) {
                    // TODO: Handle the ability to utilize the parallel execution and run jobs in parallel to each other
                    EtlJobRunner.run(job);
                }
            }
            finally {
                refreshInProgress = false;
            }
        }
    }
}
