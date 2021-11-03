package org.pih.petl.job.config;

import java.util.concurrent.TimeUnit;

/**
 * Configures how error handling is done for a job
 */
public class ErrorHandling {

    private Integer maxAttempts = 1;
    private Integer retryInterval = 5;
    private TimeUnit retryIntervalUnit = TimeUnit.MINUTES;

    public ErrorHandling() {
    }

    public ErrorHandling(Integer maxAttempts, Integer retryInterval, TimeUnit retryIntervalUnit) {
        this.maxAttempts = maxAttempts;
        this.retryInterval = retryInterval;
        this.retryIntervalUnit = retryIntervalUnit;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("maxAttempts: ").append(maxAttempts);
        if (maxAttempts > 1) {
            sb.append(" every ").append(retryInterval).append(" ").append(retryIntervalUnit);
        }
        return sb.toString();
    }

    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Integer getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Integer retryInterval) {
        this.retryInterval = retryInterval;
    }

    public TimeUnit getRetryIntervalUnit() {
        return retryIntervalUnit;
    }

    public void setRetryIntervalUnit(TimeUnit retryIntervalUnit) {
        this.retryIntervalUnit = retryIntervalUnit;
    }
}
