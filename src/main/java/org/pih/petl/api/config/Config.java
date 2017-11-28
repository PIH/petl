package org.pih.petl.api.config;

/**
 * Encapsulates the Configuration properties for the Application
 */
public class Config {

    //***** PROPERTIES *****

    private StartupJobs startupJobs;

    //***** CONSTRUCTORS *****

    public Config() {}

    //***** ACCESSORS *****


    public StartupJobs getStartupJobs() {
        return startupJobs;
    }

    public void setStartupJobs(StartupJobs startupJobs) {
        this.startupJobs = startupJobs;
    }
}
