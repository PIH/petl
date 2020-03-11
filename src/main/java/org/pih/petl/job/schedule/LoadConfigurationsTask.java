package org.pih.petl.job.schedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.ConfigFileReader;
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

    @Autowired
    ConfigFileReader jobConfigReader;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.warn("Executing Load Configurations Task");
    }
}
