package org.pih.petl.api.job;

import org.pih.petl.api.config.EtlJobConfig;

/**
 * Interface for a particular EtlJob
 */
public interface EtlJob {

    void execute() throws Exception;
}
