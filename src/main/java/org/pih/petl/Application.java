package org.pih.petl;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.schedule.JobScheduler;
import org.pih.petl.job.schedule.LoadConfigurationsTask;
import org.quartz.SimpleScheduleBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * Main class for the PETL application that starts up the Spring Boot Application
 */
@SpringBootApplication
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    @Autowired
    ApplicationConfig appConfig;

    @Autowired
    JobScheduler scheduler;

    /**
     * Run the application
     */
	public static void main(String[] args) throws Exception {
        log.info("Starting up PETL");

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        log.info("JAVA VM: " + runtimeMxBean.getVmName());
        log.info("JAVA VENDOR: " + runtimeMxBean.getSpecVendor());
        log.info("JAVA VERSION: " + runtimeMxBean.getSpecVersion() + " (" + runtimeMxBean.getVmVersion() + ")");
        log.info("JAVA_OPTS: " + runtimeMxBean.getInputArguments());

        // Initialize environment
        ApplicationContext context = SpringApplication.run(Application.class, args);
        Application app = context.getBean(Application.class);

        log.info("PETL Started Successfully");
        log.info("PETL_HOME: " + app.getAppConfig().getHomeDir());
        log.info("LOG DIR: " + app.getAppConfig().getLogFile());
        log.info("JOB CONFIG DIR: " + app.getAppConfig().getJobConfigDir());

        // Set up the schedule to check if any etl jobs need to execute every minute
        SimpleScheduleBuilder schedule = simpleSchedule().repeatForever().withIntervalInSeconds(60);
        app.getScheduler().schedule(LoadConfigurationsTask.class, schedule);
    }

    /**
     * @return the configuration of this PETL instance
     */
    public ApplicationConfig getAppConfig() {
        return appConfig;
    }

    public JobScheduler getScheduler() {
        return scheduler;
    }
}
