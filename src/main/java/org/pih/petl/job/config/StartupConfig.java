package org.pih.petl.job.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Configures how execution threading, concurrency, and retries on error should be handled
 * By default, an ExecutionConfig will specify no retries, and one concurrent job,
 * but this can be adjusted to support executing jobs in parallel, and configuring the number of times a job
 * execution should be attempted (i.e. to allow for retrying due to connectivity problems)
 */
public class StartupConfig {

    private List<String> jobs;
    private boolean exitAutomatically;

    public StartupConfig() {
    }

    public List<String> getJobs() {
        if (jobs == null) {
            jobs = new ArrayList<>();
        }
        return jobs;
    }

    public void setJobs(List<String> jobs) {
        this.jobs = jobs;
    }

    public boolean isExitAutomatically() {
        return exitAutomatically;
    }

    public void setExitAutomatically(boolean exitAutomatically) {
        this.exitAutomatically = exitAutomatically;
    }
}
