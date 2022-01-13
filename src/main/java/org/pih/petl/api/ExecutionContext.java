package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.JobConfig;

/**
 * Represents the status of a given execution
 */
public class ExecutionContext {

    private static Log log = LogFactory.getLog(ExecutionContext.class);

    private JobExecution jobExecution;
    private JobConfig jobConfig;
    private String status;

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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        log.debug(jobConfig.getType() + ": " + status);
        this.status = status;
    }
}
