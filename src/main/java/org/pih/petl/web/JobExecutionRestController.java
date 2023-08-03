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
        executeIfIncomplete(execution);
        return execution;
    }

    private JobExecution executeIfIncomplete(JobExecution execution) {
        if (execution.getStatus() != JobExecutionStatus.SUCCEEDED) {
            JobConfig config = execution.getJobConfig();
            execution.setStarted(new Date());
            execution.setCompleted(null);
            execution.setErrorMessage(null);
            execution.setStatus(JobExecutionStatus.IN_PROGRESS);
            execution = etlService.saveJobExecution(execution);

            // If this is a job pipeline or an iterating job that has already had it's child jobs scheduled,
            // then just ensure that these child jobs are executed and successful, and mark parent as successful if so
            if ("job-pipeline".equals(config.getType()) || "iterating-job".equals(config.getType())) {
                List<JobExecution> existingChildExecutions = etlService.getChildExecutions(execution);
                if (existingChildExecutions == null || existingChildExecutions.isEmpty()) {
                    execution = etlService.executeJob(execution);
                }
                else {
                    boolean successful = true;
                    for (JobExecution childJobExecution : etlService.getChildExecutions(execution)) {
                        if (childJobExecution.getStatus() != JobExecutionStatus.SUCCEEDED) {
                            childJobExecution = executeIfIncomplete(childJobExecution);
                            successful = successful && childJobExecution.getStatus() == JobExecutionStatus.SUCCEEDED;
                            if ("job-pipeline".equals(config.getType()) && !successful) {
                                break;
                            }
                        }
                    }
                    if (successful) {
                        execution.setStatus(JobExecutionStatus.SUCCEEDED);
                    } else {
                        execution.setStatus(JobExecutionStatus.FAILED);
                    }
                    execution.setCompleted(new Date());
                    execution = etlService.saveJobExecution(execution);
                }
            } else {
                execution = etlService.executeJob(execution);
            }
        }
        return execution;
    }
}
