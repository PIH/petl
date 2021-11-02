package org.pih.petl.api;

/**
 * Represents the result of executing a JobExecutionTask
 */
public class JobExecutionResult {

    private JobExecutionTask jobExecutionTask;
    private boolean successful;
    private Throwable exception;

    public JobExecutionResult(JobExecutionTask jobExecutionTask) {
        this.jobExecutionTask = jobExecutionTask;
    }

    public JobExecutionTask getJobExecutionTask() {
        return jobExecutionTask;
    }

    public void setJobExecutionTask(JobExecutionTask jobExecutionTask) {
        this.jobExecutionTask = jobExecutionTask;
    }

    public boolean shouldRetry() {
        int numAttempts = jobExecutionTask.getAttemptNum();;
        int maxAttempts = jobExecutionTask.getJobConfig().getErrorHandling().getMaxAttempts();
        return numAttempts < maxAttempts;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }
}
