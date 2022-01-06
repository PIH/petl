package org.pih.petl.job.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configures the specific PETL application defined properties
 * All other properties are either user-defined or included with Spring boot
 */
@Component
@ConfigurationProperties("petl")
public class PetlConfig {

    private String homeDir;
    private String datasourceDir;
    private String jobDir;
    private StartupConfig startup;
    private Integer maxConcurrentJobs = 1;

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

    public StartupConfig getStartup() {
        if (startup == null) {
            startup = new StartupConfig();
        }
        return startup;
    }

    public void setStartup(StartupConfig startup) {
        this.startup = startup;
    }

    public Integer getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public void setMaxConcurrentJobs(Integer maxConcurrentJobs) {
        this.maxConcurrentJobs = maxConcurrentJobs;
    }
}
