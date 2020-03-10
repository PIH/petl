package org.pih.petl.api;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

/**
 * Methods for working with the ETL Status Table.  Spring Data JPA will auto-create these methods
 */
public interface EtlStatusRepository extends CrudRepository<EtlStatus, String> {
    List<EtlStatus> findEtlStatusByJobName(String jobName);
}
