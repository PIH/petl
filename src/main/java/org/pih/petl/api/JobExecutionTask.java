package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ExecutionConfig;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private static Log log = LogFactory.getLog(JobExecutionTask.class);

    private final PetlJobConfig jobConfig;
    private final ExecutionContext context;
    private final ExecutionConfig executionConfig;

    public JobExecutionTask(PetlJobConfig jobConfig, ExecutionContext context, ExecutionConfig executionConfig) {
        this.jobConfig = jobConfig;
        this.context = context;
        this.executionConfig = executionConfig;
    }

    @Override
    public JobExecutionResult call() {
        JobExecutionResult result = new JobExecutionResult();
        int maxRetries = executionConfig.getMaxRetriesPerJob() == null ? 0 : executionConfig.getMaxRetriesPerJob();
        int retryInterval = executionConfig.getRetryInterval() == null ? 5 : executionConfig.getRetryInterval();
        TimeUnit retryUnit = executionConfig.getRetryIntervalUnit() == null ? TimeUnit.MINUTES : executionConfig.getRetryIntervalUnit();
        for (int currentAttempt = 0; !result.isSuccessful() && currentAttempt <= maxRetries; currentAttempt++) {
            context.setStatus("Executing job: " + jobConfig);
            try {
                PetlJob job = PetlJobFactory.instantiate(jobConfig);
                ExecutionContext nestedContext = new ExecutionContext(context.getJobExecution(), jobConfig, context.getApplicationConfig());
                context.setStatus("Executing job: " + job);
                job.execute(nestedContext);
                result.setSuccessful(true);
                result.setException(null);
                context.setTotalLoaded(context.getTotalLoaded() + 1);
            }
            catch (Throwable t) {
                result.setSuccessful(false);
                result.setException(t);
                StringBuilder status = new StringBuilder();
                status.append("An error occurred executing job: " + t.getMessage());
                if (currentAttempt < maxRetries) {
                    status.append(". Will retry in ").append(retryInterval).append(" ").append(retryUnit).append(" (").append(currentAttempt+1).append("/").append(maxRetries).append(")");
                    context.setStatus(status.toString());
                    try {
                        retryUnit.sleep(retryInterval);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    context.setStatus(status.toString());
                    log.error(t);
                }
            }
        }
        return result;
    }
}
