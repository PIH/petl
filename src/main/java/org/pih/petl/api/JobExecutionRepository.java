package org.pih.petl.api;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

/**
 * Methods for working with the petl_job_execution table.  Spring Data JPA will auto-create these methods
 */
public interface JobExecutionRepository extends CrudRepository<JobExecution, String> {
    List<JobExecution> findJobExecutionsByJobPathIsNotNullOrderByInitiatedDesc();
    List<JobExecution> findJobExecutionByJobPathOrderByStartedDesc(String jobPath);
    JobExecution findFirstByJobPathOrderByStartedDesc(String jobPath);
    List<JobExecution> findJobExecutionsByCompletedIsNull();
    List<JobExecution> findJobExecutionsByParentExecutionUuidEqualsOrderBySequenceNum(String uuid);
    JobExecution getJobExecutionByUuid(String uuid);

    @Modifying
    @Transactional
    @Query("UPDATE petl_job_execution e SET e.completed = :now, e.status = :status WHERE e.completed IS NULL")
    void markAllHungJobsAborted(@Param("now") Date now, @Param("status") JobExecutionStatus status);
}
