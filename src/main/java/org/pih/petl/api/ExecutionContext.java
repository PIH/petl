package org.pih.petl.api;

import org.pih.petl.job.config.JobConfig;

/**
 * Represents the status of a given execution
 */
public class ExecutionContext {

    private JobExecution jobExecution;
    private JobConfig jobConfig;

    public ExecutionContext(JobExecution jobExecution, JobConfig jobConfig) {
        this.jobExecution = jobExecution;
        this.jobConfig = jobConfig;
    }

    public JobExecution getJobExecution() {
        return jobExecution;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }
}
