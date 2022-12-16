package org.pih.petl.job;

import org.pih.petl.ApplicationConfig;
import org.pih.petl.api.JobExecution;

/**
 * Base abstract class for Petl Jobs
 */
public abstract class AbstractJob implements PetlJob {

    ApplicationConfig applicationConfig;

    public AbstractJob(ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
    }
    
}
