package org.pih.petl.job;

import org.pih.petl.api.JobExecution;

/**
 * Interface for a particular PetlJob
 */
public interface PetlJob {

    void execute(JobExecution jobExecution) throws Exception;
}
