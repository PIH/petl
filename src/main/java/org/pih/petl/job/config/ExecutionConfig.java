package org.pih.petl.job.config;

import java.sql.Time;
import java.util.concurrent.TimeUnit;

/**
 * Configures how execution threading, concurrency, and retries on error should be handled
 * By default, an ExecutionConfig will specify no retries, and one concurrent job,
 * but this can be adjusted to support executing jobs in parallel, and configuring the number of times a job
 * execution should be attempted (i.e. to allow for retrying due to connectivity problems)
 */
public class ExecutionConfig {

    private Integer maxConcurrentJobs = 1;
    private Integer maxRetriesPerJob = 0;
    private Integer retryInterval = 5;
    private TimeUnit retryIntervalUnit = TimeUnit.MINUTES;

    public ExecutionConfig() {
    }

    public ExecutionConfig(Integer maxConcurrentJobs, Integer maxRetriesPerJob, Integer retryInterval, TimeUnit retryIntervalUnit) {
        this.maxConcurrentJobs = maxConcurrentJobs;
        this.maxRetriesPerJob = maxRetriesPerJob;
        this.retryInterval = retryInterval;
        this.retryIntervalUnit = retryIntervalUnit;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("maxConcurrentJobs: ").append(maxConcurrentJobs).append(", retries: ").append(maxRetriesPerJob);
        if (maxRetriesPerJob > 0) {
            sb.append(" every ").append(retryInterval).append(" ").append(retryIntervalUnit);
        }
        return sb.toString();
    }

    public Integer getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public void setMaxConcurrentJobs(Integer maxConcurrentJobs) {
        this.maxConcurrentJobs = maxConcurrentJobs;
    }

    public Integer getMaxRetriesPerJob() {
        return maxRetriesPerJob;
    }

    public void setMaxRetriesPerJob(Integer maxRetriesPerJob) {
        this.maxRetriesPerJob = maxRetriesPerJob;
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
