package org.pih.petl.api;

/**
 * Represents the result of executing a JobExecutionTask
 */
public class JobExecutionResult {

    private boolean successful;
    private Throwable exception;

    public JobExecutionResult() {
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
