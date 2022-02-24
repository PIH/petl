package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.PetlJob;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private static final Log log = LogFactory.getLog(EtlService.class);

    private final EtlService etlService;
    private final JobExecution jobExecution;
    private int attemptNum = 1;

    public JobExecutionTask(EtlService etlService, JobExecution jobExecution) {
        this.etlService = etlService;
        this.jobExecution = jobExecution;
    }

    @Override
    public String toString() {
        return jobExecution+ " (#" + attemptNum + ")";
    }

    @Override
    public JobExecutionResult call() {
        JobExecutionResult result = new JobExecutionResult(this);
        jobExecution.setStarted(new Date());
        jobExecution.setStatus(JobExecutionStatus.IN_PROGRESS);
        etlService.saveJobExecution(jobExecution);
         try {
             log.info(jobExecution);
             log.info("Job (" + jobExecution.getUuid() + "): " + jobExecution.getJobConfig());
             PetlJob petlJob = etlService.getPetlJob(jobExecution.getJobConfig());
             petlJob.execute(jobExecution);
             result.setSuccessful(true);
             result.setException(null);
        }
        catch (Throwable t) {
            result.setSuccessful(false);
            result.setException(t);
            log.error("Execution Failed for job: " + jobExecution.getJobConfig(), t);
        }
        return result;
    }

    public JobExecution getJobExecution() {
        return jobExecution;
    }

    public int getAttemptNum() {
        return attemptNum;
    }

    public void incrementAttemptNum() {
        this.attemptNum++;
    }
}
