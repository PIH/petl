package org.pih.petl.job.type;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;

/**
 * Encapsulates a particular ETL job configuration
 */
public class RunMultipleJob implements PetlJob {

    private static Log log = LogFactory.getLog(RunMultipleJob.class);

    /**
     * Creates a new instance of the job
     */
    public RunMultipleJob() {
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        ApplicationConfig appConfig = context.getApplicationConfig();
        PetlJobConfig config = context.getJobConfig();
        List<String> jobs = config.getStringList("jobs");
        boolean parallelExecution = config.getBoolean("parallelExecution");
        context.setStatus("Executing " + jobs.size() + " in " + (parallelExecution ? "parallel" : "series"));
        context.setTotalExpected(jobs.size());

        // TODO: Handle the ability to utilize the parallel execution and run jobs in parallel to each other
        for (String jobPath : jobs) {
            context.setStatus("Running job: " + jobPath);
            PetlJobConfig nestedJobConfig = appConfig.getPetlJobConfig(jobPath);
            ExecutionContext nestedContext = new ExecutionContext(context.getJobExecution(), nestedJobConfig, appConfig);
            PetlJob job = PetlJobFactory.instantiate(nestedJobConfig);
            job.execute(nestedContext);
            // TODO: Periodically update overall context status from nested status, using thread.  See SqlServerImportJob
            context.setTotalLoaded(context.getTotalLoaded() + 1);
        }
    }
}
