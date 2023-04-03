package org.pih.petl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.api.JobScheduler;
import org.pih.petl.api.ScheduledExecutionTask;
import org.pih.petl.job.config.PetlConfig;
import org.quartz.SimpleScheduleBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/**
 * Main class for the PETL application that starts up the Spring Boot Application
 */
@SpringBootApplication
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    final ApplicationConfig appConfig;
    final JobScheduler scheduler;
    final EtlService etlService;

    @Autowired
    public Application(ApplicationConfig appConfig, JobScheduler scheduler, EtlService etlService) {
        this.appConfig = appConfig;
        this.scheduler = scheduler;
        this.etlService = etlService;
    }

    /**
     * Run the application
     */
	public static void main(String[] args) throws Exception {
        log.info("Starting up PETL");

        PetlExitCodeGenerator exitCodeGenerator = new PetlExitCodeGenerator();

        // Initialize environment
        ApplicationContext context = SpringApplication.run(Application.class, args);
        Application app = context.getBean(Application.class);

        try {
            log.info("PETL Starting Up");

            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            log.info("JAVA VM: " + runtimeMxBean.getVmName());
            log.info("JAVA VENDOR: " + runtimeMxBean.getSpecVendor());
            log.info("JAVA VERSION: " + runtimeMxBean.getSpecVersion() + " (" + runtimeMxBean.getVmVersion() + ")");
            log.info("JAVA_OPTS: " + runtimeMxBean.getInputArguments());
            log.info("PETL_HOME: " + app.getAppConfig().getPetlHomeDir());
            log.info("JOB DIR: " + app.getAppConfig().getJobDir());
            log.info("DATASOURCE DIR: " + app.getAppConfig().getDataSourceDir());

            // Reset any hung jobs
            app.getEtlService().markHungJobsAsRun();

            PetlConfig petlConfig = app.getAppConfig().getPetlConfig();

            // Get and execute any jobs configured to run at startup
            List<String> startupJobs = petlConfig.getStartup().getJobs();
            log.info("STARTUP JOBS: " + startupJobs);
            if (!startupJobs.isEmpty()) {
                JobExecutor jobExecutor = new JobExecutor(app.getEtlService(), petlConfig.getMaxConcurrentJobs());
                try {
                    for (String job : startupJobs) {
                        jobExecutor.executeJob(job);
                    }
                }
                finally {
                    jobExecutor.shutdown();
                }
            }
        }
        catch (Exception e) {
            exitCodeGenerator.addException(e);
            throw e;
        }
        finally {
            // If configured to exist after startup jobs, exit application
            if (app.getAppConfig().getPetlConfig().getStartup().isExitAutomatically()) {
                log.info("PETL Shutting Down");
                int exitCode = SpringApplication.exit(context, exitCodeGenerator);
                for (Throwable exception : exitCodeGenerator.getExceptions()) {
                    log.error(exception);
                }
                System.exit(exitCode);
            }
        }

        // Set up the schedule to check if any etl jobs need to execute every minute
        SimpleScheduleBuilder schedule = simpleSchedule().repeatForever().withIntervalInSeconds(60);
        app.getScheduler().schedule(ScheduledExecutionTask.class, schedule, 10000);  // Delay 10 seconds
        log.info("PETL running");
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
