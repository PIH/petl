package org.pih.petl.api;

import java.util.Date;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.PetlJobFactory;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Methods for working with the ETL Status Table
 */
@Service
public class EtlService {

    private static Log log = LogFactory.getLog(EtlService.class);

    @Autowired
    JobConfigReader jobConfigReader;

    @Autowired
    EtlStatusRepository repository;

    @Transactional
    public EtlStatus createStatus(String jobName) {
        for (EtlStatus previousStatus : repository.findEtlStatusByJobName(jobName)) {
            previousStatus.setNum(previousStatus.getNum() + 1);
            repository.save(previousStatus);
        }
        EtlStatus newStatus = new EtlStatus(UUID.randomUUID().toString(), jobName);
        newStatus.setNum(1);
        newStatus.setStarted(new Date());
        newStatus.setStatus("Refresh initiated");
        repository.save(newStatus);
        log.debug(newStatus);
        return newStatus;
    }

    @Transactional
    public EtlStatus updateEtlStatus(EtlStatus etlStatus) {
        etlStatus = repository.save(etlStatus);
        log.debug(etlStatus);
        return etlStatus;
    }

    public void executeJob(String jobPath) {
        PetlJob job = PetlJobFactory.instantiate(this, jobConfigReader, jobPath);
        job.execute();
    }
}
