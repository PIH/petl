package org.pih.petl.web;

import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
import org.pih.petl.api.JobExecutionStatus;
import org.pih.petl.job.config.JobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;

@RestController
@EnableAutoConfiguration
public class JobExecutionRestController {

    @Autowired
    EtlService etlService;

    @GetMapping("/execution")
    List<JobExecution> getJobExecutions() {
        return etlService.getJobExecutionsAtTopLevel();
    }

    @GetMapping("/execution/{uuid}")
    JobExecution getJobExecution(@PathVariable String uuid) {
        return etlService.getJobExecution(uuid);
    }

    @GetMapping("/execution/{uuid}/children")
    List<JobExecution> getChildExecutions(@PathVariable String uuid) {
        JobExecution execution = etlService.getJobExecution(uuid);
        return etlService.getChildExecutions(execution);
    }

    @PostMapping("/execution/{uuid}")
    JobExecution executeJob(@PathVariable String uuid) {
        JobExecution execution = etlService.getJobExecution(uuid);
        return etlService.executeIfIncomplete(execution);
    }
}
