package org.pih.petl.api;

import java.util.Date;

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
     * @param jobType the job type
     * @param schedule the schedule builder
     * @param delayMs the delay
     * @throws SchedulerException if an error occurs
     */
    public void schedule(Class<? extends Job> jobType, ScheduleBuilder schedule, long delayMs) throws SchedulerException {
        Date start = new Date(System.currentTimeMillis() + delayMs);
        JobDetail jobDetail = JobBuilder.newJob().ofType(jobType).withIdentity(jobType.getSimpleName()).build();
        Trigger jobTrigger = TriggerBuilder.newTrigger().forJob(jobDetail).withSchedule(schedule).startAt(start).build();
        scheduler.scheduleJob(jobDetail, jobTrigger);
    }
}
