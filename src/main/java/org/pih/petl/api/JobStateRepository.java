package org.pih.petl.api;

import org.springframework.data.repository.CrudRepository;

/**
 * Methods for working with the petl_job_execution table.  Spring Data JPA will auto-create these methods
 */
public interface JobStateRepository extends CrudRepository<JobState, JobState.Key> {

}
