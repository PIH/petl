package org.pih.petl.job.schedule;

import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.PetlJobConfig;
import org.quartz.CronExpression;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Schedulable task for loading all of the configurations
 */
@Component
public class LoadConfigurationsTask implements Job {

    private static final Log log = LogFactory.getLog(LoadConfigurationsTask.class);

    private static boolean inProgress = false;

    @Autowired
    EtlService etlService;

    /**
     * Load all jobs from file system
     * For each job found:
     *  - Get the job path and query DB for last execution start and end date
     *  - If it has never been executed, then execute it
     *  - Else if it is currently executing, then skip
     *  - Else, if there is a cron expression configured, check if next scheduled date following
     *    last execution date is <= now, and if so, execute it
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (!inProgress) {
            try {
                inProgress = true;
                Date currentDate = new Date();
                log.debug("Executing Load Configurations Task: " + currentDate);
                Map<String, PetlJobConfig> jobs = etlService.getAllConfiguredJobs();
                log.debug("Found " + jobs.size() + " configured Jobs");
                for (String jobPath : jobs.keySet()) {
                    log.debug("Checking job: " + jobPath);
                    JobExecution latestExecution = etlService.getLatestJobExecution(jobPath);
                    if (latestExecution == null) {
                        log.debug("Job has never been executed, executing now");
                        etlService.executeJob(jobPath);
                    }
                    else {
                        Date lastStartDate = latestExecution.getStarted();
                        Date lastEndDate = latestExecution.getCompleted();
                        log.debug("Job has previously been executed: " + lastStartDate + " - " + lastEndDate);
                        if (lastEndDate != null) {
                            PetlJobConfig jobConfig = jobs.get(jobPath);
                            Schedule schedule = jobConfig.getSchedule();
                            if (schedule != null) {
                                log.debug("Schedule found: " + schedule);
                                CronExpression cronExpression = new CronExpression(schedule.getCron());
                                log.debug("Scheduled based on cron: " + cronExpression.getExpressionSummary());
                                Date nextScheduledDate = cronExpression.getNextValidTimeAfter(lastStartDate);
                                log.debug("Next scheduled for: " + nextScheduledDate);
                                if (nextScheduledDate != null && nextScheduledDate.compareTo(currentDate) <= 0) {
                                    log.debug("Schedule is satisfied, executing task");
                                    etlService.executeJob(jobPath);
                                }
                                else {
                                    log.debug("This is in the future, not executing");
                                }
                            }
                            else {
                                log.debug("Job has no schedule associated with it, not executing.");
                            }
                        }
                        else {
                            log.debug("Latest execution is still in progress, not running again in parallel");
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("An error occured while executing a scheduled job.  Aborting all remaining jobs.", e);
            }
            finally {
                inProgress = false;
            }
        }
        else {
            log.trace("Load Configurations Task already in progress, not running again in parallel");
        }
    }


}
