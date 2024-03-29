package org.pih.petl.api;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.Schedule;
import org.quartz.CronExpression;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Date;
import java.util.Map;

/**
 * Schedulable task for loading all of the configurations
 */
@Component
public class ScheduledExecutionTask implements Job {

    private static final Log log = LogFactory.getLog(ScheduledExecutionTask.class);

    private static boolean enabled = true;
    private static boolean inProgress = false;

    final EtlService etlService;
    final JobExecutor jobExecutor;

    @Autowired
    public ScheduledExecutionTask(EtlService etlService) {
        this.etlService = etlService;
        this.jobExecutor = new JobExecutor(etlService, etlService.getApplicationConfig().getPetlConfig().getMaxConcurrentJobs());
    }

    @PreDestroy
    public  void onShutdown() {
        if (jobExecutor != null) {
            jobExecutor.shutdown();
        }
    }

    /**
     * Load all jobs from file system
     * For each job found:
     *  - Get the job path and query DB for last execution start and end date
     *  - If it has never been executed, then execute it
     *  - Else if it is currently executing ("completed" is null), then skip
     *  - Else, if there is a cron expression configured, check if next scheduled date following
     *    last execution date is <= now, and if so, execute it
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (enabled && !inProgress) {
            try {
                inProgress = true;
                Date currentDate = jobExecutionContext.getFireTime();
                log.trace("Executing Task: " + currentDate);
                Map<String, JobConfig> jobs = etlService.getAllConfiguredJobs();
                log.trace("Found " + jobs.size() + " configured Jobs");
                for (String jobPath : jobs.keySet()) {
                    log.trace("Checking job: " + jobPath);
                    JobConfig jobConfig = jobs.get(jobPath);
                    Schedule schedule = jobConfig.getSchedule();
                    boolean isScheduled = schedule != null && StringUtils.isNotBlank(schedule.getCron());
                    if (isScheduled) {
                        JobExecution latestExecution = etlService.getLatestJobExecution(jobPath);
                        if (latestExecution == null) {
                            log.debug("Job: " + jobPath + " - Executing for the first time.");
                            jobExecutor.executeJob(jobPath);
                        }
                        else {
                            Date lastStartDate = latestExecution.getStarted();
                            log.trace("Last Execution Start: " + lastStartDate);
                            Date lastEndDate = latestExecution.getCompleted();
                            log.trace("Last Execution End: " + lastEndDate);
                            if (lastEndDate != null) {
                                log.trace("Schedule found: " + schedule);
                                CronExpression cronExpression = new CronExpression(schedule.getCron());
                                Date nextScheduledDate = cronExpression.getNextValidTimeAfter(lastStartDate);
                                log.trace("Next scheduled for: " + nextScheduledDate);
                                if (nextScheduledDate != null && nextScheduledDate.compareTo(currentDate) <= 0) {
                                    log.debug("Executing scheduled job: " + jobPath);
                                    log.trace("Last run: " + lastStartDate);
                                    JobExecution execution = jobExecutor.executeJob(jobPath);
                                    nextScheduledDate = cronExpression.getNextValidTimeAfter(execution.getStarted());
                                    log.trace("Scheduled job will run again at: " + nextScheduledDate);
                                }
                                else {
                                    log.trace("This is in the future, not executing");
                                }
                            }
                            else {
                                log.trace("Latest execution is still in progress, not running again in parallel");
                            }
                        }
                    }
                    else {
                        log.trace("Job has no scheduled associated with it, not executing.");
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

    public static boolean isEnabled() {
        return enabled;
    }

    public static void setEnabled(boolean enabled) {
        ScheduledExecutionTask.enabled = enabled;
    }
}
