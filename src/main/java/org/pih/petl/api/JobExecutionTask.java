package org.pih.petl.api;

import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfig;

import java.util.concurrent.Callable;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private final ExecutionContext executionContext;
    private int attemptNum = 1;

    public JobExecutionTask(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public String toString() {
        return getJobConfig() + " (#" + attemptNum + ")";
    }

    @Override
    public JobExecutionResult call() {
        JobExecutionResult result = new JobExecutionResult(this);
         try {
             PetlJob job = JobFactory.instantiate(executionContext.getJobConfig());
             if (job == null) {
                 throw new PetlException("Unable to find job of type: " + executionContext.getJobConfig().getType());
             }
             job.execute(executionContext);
             result.setSuccessful(true);
             result.setException(null);
        }
        catch (Throwable t) {
            result.setSuccessful(false);
            result.setException(t);
        }
        return result;
    }

    public JobConfig getJobConfig() {
        return executionContext.getJobConfig();
    }

    public int getAttemptNum() {
        return attemptNum;
    }

    public void incrementAttemptNum() {
        this.attemptNum++;
    }
}
