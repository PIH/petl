package org.pih.petl.job.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configures how execution threading, concurrency, and retries on error should be handled
 * By default, an ExecutionConfig will specify no retries, and one concurrent job,
 * but this can be adjusted to support executing jobs in parallel, and configuring the number of times a job
 * execution should be attempted (i.e. to allow for retrying due to connectivity problems)
 */
@Component
@ConfigurationProperties("petl")
public class PetlConfig {

    private String homeDir;
    private String datasourceDir;
    private String jobDir;
    private Schedule schedule;
    private StartupConfig startup;
    private ExecutionConfig execution;

    public PetlConfig() {
    }

    public String getHomeDir() {
        return homeDir;
    }

    public void setHomeDir(String homeDir) {
        this.homeDir = homeDir;
    }

    public String getDatasourceDir() {
        return datasourceDir;
    }

    public void setDatasourceDir(String datasourceDir) {
        this.datasourceDir = datasourceDir;
    }

    public String getJobDir() {
        return jobDir;
    }

    public void setJobDir(String jobDir) {
        this.jobDir = jobDir;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public StartupConfig getStartup() {
        return startup;
    }

    public void setStartup(StartupConfig startup) {
        this.startup = startup;
    }

    public ExecutionConfig getExecution() {
        return execution;
    }

    public void setExecution(ExecutionConfig execution) {
        this.execution = execution;
    }
}
