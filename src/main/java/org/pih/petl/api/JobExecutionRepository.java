package org.pih.petl.api;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

/**
 * Methods for working with the petl_job_execution table.  Spring Data JPA will auto-create these methods
 */
public interface JobExecutionRepository extends CrudRepository<JobExecution, String> {
    List<JobExecution> findJobExecutionByJobPathOrderByStartedDesc(String jobPath);
}
