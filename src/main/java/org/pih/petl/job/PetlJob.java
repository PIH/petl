package org.pih.petl.job;

import org.pih.petl.api.ExecutionContext;

/**
 * Interface for a particular PetlJob
 */
public interface PetlJob {

    void execute(ExecutionContext context) throws Exception;
}
