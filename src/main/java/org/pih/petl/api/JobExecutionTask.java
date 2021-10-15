package org.pih.petl.api;

import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private List<PetlJobConfig> jobConfigs;
    private ExecutionContext context;
    private Integer maxRetries;
    private Integer retryIntervalSeconds;

    public JobExecutionTask(List<PetlJobConfig> jobConfigs, ExecutionContext context, Integer maxRetries, Integer retryIntervalSeconds) {
        this.jobConfigs = jobConfigs;
        this.context = context;
        this.maxRetries = maxRetries;
        this.retryIntervalSeconds = retryIntervalSeconds;
    }

    @Override
    public JobExecutionResult call() {
        JobExecutionResult result = new JobExecutionResult();
        int maxAttempts = maxRetries == null ? 0 : maxRetries;
        long retryInterval = (retryIntervalSeconds != null ? retryIntervalSeconds.longValue() : 60*5);
        for (int currentAttempt = 0; !result.isSuccessful() && currentAttempt <= maxAttempts; currentAttempt++) {
            context.setStatus("Executing job list: " + jobConfigs);
            try {
                for (PetlJobConfig jobConfig : jobConfigs) {
                    PetlJob job = PetlJobFactory.instantiate(jobConfig);
                    ExecutionContext nestedContext = new ExecutionContext(context.getJobExecution(), jobConfig, context.getApplicationConfig());
                    context.setStatus("Executing job: " + job);
                    job.execute(nestedContext);
                }
                result.setSuccessful(true);
                result.setException(null);
            }
            catch (Throwable t) {
                context.setStatus("An error occurred executing job.  This was attempt " + (currentAttempt+1) + "/" + maxAttempts + ". Will retry in " + retryInterval + " seconds");
                result.setException(t);
                try {
                    TimeUnit.SECONDS.sleep(retryInterval);
                }
                catch (InterruptedException e) {}
            }
        }
        return result;
    }
}
