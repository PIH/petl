package org.pih.petl.job.schedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

/**
 * Schedulable task for loading all of the configurations
 */
@Component
public class SchedulerTestTask implements Job {

    private static final Log log = LogFactory.getLog(SchedulerTestTask.class);

    public static long numExecutions;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        numExecutions++;
        log.debug("Executing Task: " + " #" + numExecutions);
    }
}
