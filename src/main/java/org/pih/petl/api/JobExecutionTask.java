package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.JobFailedException;
import org.pih.petl.LogUtils;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ErrorHandling;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Represents an ETL job execution and the status of this
 */
public class JobExecutionTask implements Callable<JobExecutionResult> {

    private static final Log log = LogFactory.getLog(JobExecutionTask.class);

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
        String previousJobContext = LogUtils.setJobContext(getJobContext());
        long startMillis = System.currentTimeMillis();
        try {
             // Saving is inside the try so that a failure is returned as a failed result, subject to retry
             jobExecution.setStarted(new Date());
             jobExecution.setStatus(JobExecutionStatus.IN_PROGRESS);
             etlService.saveJobExecution(jobExecution);
             RunMonitor monitor = etlService.getRunMonitor();
             if (monitor != null) {
                 monitor.onJobStart(jobExecution, etlService);
             }
             log.info("Started" + attemptDescription());
             log.debug(jobExecution);
             log.debug("Job configuration: " + jobExecution.getJobConfig());
             PetlJob petlJob = etlService.getPetlJob(jobExecution.getJobConfig());
             petlJob.execute(jobExecution);
             result.setSuccessful(true);
             result.setException(null);
             log.info("Succeeded in " + LogUtils.formatDuration(System.currentTimeMillis() - startMillis));
        }
        catch (Throwable t) {
            result.setSuccessful(false);
            result.setException(t);
            logFailure(t, System.currentTimeMillis() - startMillis);
        }
        finally {
            LogUtils.setJobContext(previousJobContext);
        }
        return result;
    }

    /**
     * Logs a failure as a single line.  The stack trace is only included on the final attempt of the job in which the
     * failure originated, and not for retries or for parent jobs that failed due to a child job failure.
     */
    private void logFailure(Throwable t, long durationMillis) {
        ErrorHandling errorHandling = getErrorHandling();
        boolean willRetry = attemptNum < errorHandling.getMaxAttempts();
        StringBuilder msg = new StringBuilder("Failed").append(attemptDescription());
        if (willRetry) {
            msg.append(", retrying in ").append(errorHandling.getRetryInterval()).append(" ").append(errorHandling.getRetryIntervalUnit());
        }
        msg.append(" after ").append(LogUtils.formatDuration(durationMillis)).append(": ").append(LogUtils.summarizeException(t));
        if (willRetry) {
            log.warn(msg);
        }
        else if (t instanceof JobFailedException) {
            log.error(msg);
        }
        else {
            log.error(msg, t);
        }
    }

    /**
     * @return the job path or description, or if neither is configured, the job type and sequence number
     */
    private String getJobContext() {
        String label = RunSummaryLogger.label(jobExecution);
        if (jobExecution.getJobPath() == null && jobExecution.getDescription() == null) {
            try {
                label = jobExecution.getJobConfig().getType();
                if (jobExecution.getSequenceNum() != null) {
                    label += " #" + jobExecution.getSequenceNum();
                }
            }
            catch (Exception e) {
                // Use the default label
            }
        }
        return label;
    }

    private String attemptDescription() {
        int maxAttempts = getErrorHandling().getMaxAttempts();
        return maxAttempts > 1 ? " (attempt " + attemptNum + " of " + maxAttempts + ")" : "";
    }

    private ErrorHandling getErrorHandling() {
        try {
            return jobExecution.getJobConfig().getErrorHandling();
        }
        catch (Exception e) {
            return new ErrorHandling();
        }
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
