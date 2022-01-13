package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfig;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private static final Log log = LogFactory.getLog(EtlService.class);

    private final EtlService etlService;
    private final PetlJob petlJob;
    private final ExecutionContext executionContext;
    private int attemptNum = 1;

    public JobExecutionTask(EtlService etlService, PetlJob petlJob, ExecutionContext executionContext) {
        this.etlService = etlService;
        this.petlJob = petlJob;
        this.executionContext = executionContext;
    }

    @Override
    public String toString() {
        return executionContext.getJobExecution() + " (#" + attemptNum + ")";
    }

    @Override
    public JobExecutionResult call() {
        JobExecutionResult result = new JobExecutionResult(this);
        JobExecution execution = executionContext.getJobExecution();
        execution.setStarted(new Date());
        execution.setStatus(JobExecutionStatus.IN_PROGRESS);
        etlService.saveJobExecution(execution);
         try {
             log.info(executionContext.getJobExecution());
             log.info("Job (" + executionContext.getJobExecution().getUuid() + "): " + executionContext.getJobConfig());
             petlJob.execute(executionContext);
             result.setSuccessful(true);
             result.setException(null);
        }
        catch (Throwable t) {
            result.setSuccessful(false);
            result.setException(t);
            log.error("Execution Failed for job: " + executionContext.getJobConfig(), t);
        }
        return result;
    }

    public PetlJob getPetlJob() {
        return petlJob;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
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
