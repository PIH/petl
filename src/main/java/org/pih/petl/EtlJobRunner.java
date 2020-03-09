package org.pih.petl;

import org.pih.petl.api.config.ConfigLoader;
import org.pih.petl.api.config.EtlJobConfig;
import org.pih.petl.api.job.EtlJob;
import org.pih.petl.api.job.LoadSqlServerJob;
import org.pih.petl.api.job.RunMultipleJob;

/**
 * Encapsulates a runnable pipeline
 */
public class EtlJobRunner {

    /**
     * Run the given job specified at the configured path
     */
    public static void run(String jobPath) throws Exception {
        EtlJobConfig etlJobConfig = ConfigLoader.getEtlJobConfigFromFile(jobPath);
        run(etlJobConfig);
    }

    /**
     * Run the given job given the specified configuration
     */
    public static void run(EtlJobConfig etlJobConfig) throws Exception {
        EtlJob job = newJobInstance(etlJobConfig);
        if (job != null) {
            job.execute();
        }
        else {
            throw new IllegalStateException("Unable to find a instantiate job from configuration");
        }
    }

    public static EtlJob newJobInstance(EtlJobConfig etlJobConfig) {
        if ("run-pipeline".equals(etlJobConfig.getType())) {
            return new RunMultipleJob(etlJobConfig);

        }
        else if ("load-sqlserver".equals(etlJobConfig.getType())) {
            return new LoadSqlServerJob(etlJobConfig);
        }
        else {
            throw new IllegalArgumentException("Invalid job type of " + etlJobConfig.getType());
        }
    }
}
