package org.pih.petl.job.schedule;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Wraps the methods of the schedule
 */
@Component
public class JobScheduler {

    @Autowired
    Scheduler scheduler;

    /**
     * Schedules the given job class to execute at the given interval in seconds
     */
    public void schedule(Class<? extends Job> jobType, ScheduleBuilder schedule) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob().ofType(jobType).withIdentity(jobType.getSimpleName()).build();
        Trigger jobTrigger = TriggerBuilder.newTrigger().forJob(jobDetail).withSchedule(schedule).build();
        scheduler.scheduleJob(jobDetail, jobTrigger);
    }
}
