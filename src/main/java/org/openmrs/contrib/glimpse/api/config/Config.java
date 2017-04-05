package org.openmrs.contrib.glimpse.api.config;

import java.util.List;

/**
 * Encapsulates the Configuration properties for the Glimpse Application
 */
public class Config {

    //***** PROPERTIES *****

    private TargetEnvironment targetEnvironment;
    private List<SourceEnvironment> sourceEnvironments;

    //***** CONSTRUCTORS *****

    public Config() {}

    //***** ACCESSORS *****

    public TargetEnvironment getTargetEnvironment() {
        return targetEnvironment;
    }

    public void setTargetEnvironment(TargetEnvironment targetEnvironment) {
        this.targetEnvironment = targetEnvironment;
    }

    public List<SourceEnvironment> getSourceEnvironments() {
        return sourceEnvironments;
    }

    public void setSourceEnvironments(List<SourceEnvironment> sourceEnvironments) {
        this.sourceEnvironments = sourceEnvironments;
    }
}
