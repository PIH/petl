package org.pih.petl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.schedule.JobScheduler;
import org.pih.petl.job.schedule.PetlScheduledExecutionTask;
import org.quartz.SimpleScheduleBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

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

    @Autowired
    EtlService etlService;

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
        log.info("PETL_HOME: " + app.getAppConfig().getPetlHomeDir());
        log.info("JOB DIR: " + app.getAppConfig().getJobDir());
        log.info("DATASOURCE DIR: " + app.getAppConfig().getDataSourceDir());

        // Reset any hung jobs
        app.getEtlService().markHungJobsAsRun();

        // Set up the schedule to check if any etl jobs need to execute every minute
        SimpleScheduleBuilder schedule = simpleSchedule().repeatForever().withIntervalInSeconds(60);
        app.getScheduler().schedule(PetlScheduledExecutionTask.class, schedule, 10000);  // Delay 10 seconds
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

    public EtlService getEtlService() { return etlService; }
}
