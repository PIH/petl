package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.job.config.JobConfig;

/**
 * Represents the status of a given execution
 */
public class ExecutionContext {

    private static Log log = LogFactory.getLog(ExecutionContext.class);

    private JobExecution jobExecution;
    private JobConfig jobConfig;
    private ApplicationConfig applicationConfig;
    private String status;
    private int totalExpected;
    private int totalLoaded;

    public ExecutionContext(JobExecution jobExecution, JobConfig jobConfig, ApplicationConfig applicationConfig) {
        this.jobExecution = jobExecution;
        this.jobConfig = jobConfig;
        this.applicationConfig = applicationConfig;
    }

    public JobExecution getJobExecution() {
        return jobExecution;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        log.debug(jobConfig.getType() + ": " + status);
        this.status = status;
    }

    public int getTotalExpected() {
        return totalExpected;
    }

    public void setTotalExpected(int totalExpected) {
        this.totalExpected = totalExpected;
    }

    public int getTotalLoaded() {
        return totalLoaded;
    }

    public void setTotalLoaded(int totalLoaded) {
        this.totalLoaded = totalLoaded;
        log.trace(jobConfig.getType() + ": " + status + " loaded " + totalLoaded + "/" + totalExpected);
    }
}
