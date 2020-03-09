package org.pih.petl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.Result;
import org.pih.petl.api.JobRunner;
import org.pih.petl.api.config.Config;
import org.pih.petl.api.status.EtlStatusTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;

/**
 * This mainly just serves to demonstrate that we can start up and run our Spring Boot application.
 * Run this class from Intellij, and see it start up without errors
 *
 * The Configuration, EnableConfigurationProperties, and ConfigurationProperties annotations work to
 * enable the use of an application.properties or application.yml file to pass in configuration to the application.
 * There is good documentation on how we can set this up to have overrides in various environments here:
 * http://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html
 *
 * These can go on any class (eg. we could have a new Configuration bean, but the application bean is often used by convention
 *
 * The below properties are all able to be set via these configuration files
 */
@SpringBootApplication
@EnableConfigurationProperties
@Configuration
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    public static final String ENV_PETL_HOME = "PETL_HOME";

    @ConfigurationProperties
    @Bean
    Config getConfig() {
        return new Config();
    }

    /**
     * Run the application
     */
	public static void main(String[] args) {

        log.info("Starting up PETL");

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        log.info("JAVA VM: " + runtimeMxBean.getVmName());
        log.info("JAVA VENDOR: " + runtimeMxBean.getSpecVendor());
        log.info("JAVA VERSION: " + runtimeMxBean.getSpecVersion() + " (" + runtimeMxBean.getVmVersion() + ")");
        log.info("JAVA_OPTS: " + runtimeMxBean.getInputArguments());

	    // Initialize environment
        File petlHomeDir = getHomeDir();
        log.info("PETL_HOME: " + petlHomeDir);

        File petlLogFile = getLogFile();
        log.info("LOGGING TO: " + petlLogFile);

        ApplicationContext context = SpringApplication.run(Application.class, args);
        Application app = context.getBean(Application.class);

        // If the target status table does not exist, create it here
        EtlStatusTable.createStatusTable();

        // If any startup jobs are defined, execute these
        PetlExitCodeGenerator exitCodeGenerator = new PetlExitCodeGenerator();
        try {
            for (String jobConfigFile : app.getConfig().getStartupJobs().getJobs()) {
                Properties jobConfig = PetlUtil.loadPropertiesFromFile(jobConfigFile);
                JobRunner jobRunner = new JobRunner();
                jobRunner.setConfiguration(jobConfig);
                Result result = jobRunner.runJob();
                if (result.getNrErrors() > 0) {
                    throw new RuntimeException("One or more errors was detected in job result: " + result.getLogText());
                }
            }
        }
        catch (Exception e) {
            exitCodeGenerator.addException(e);
            throw new RuntimeException("Unable to execute job", e);
        }
        finally {
            // If configured to exist after startup jobs, exit application
            if (app.getConfig().getStartupJobs().isExitAutomatically()) {
                int exitCode = SpringApplication.exit(context, exitCodeGenerator);
                for (Throwable exception : exitCodeGenerator.getExceptions()) {
                    log.error(exception);
                }
                System.exit(exitCode);
            }
        }
	}

    /**
     * @return the path to where PETL is installed, defined by the PETL_HOME environment variable
     */
    public static String getHomePath() {
        String path = System.getenv(ENV_PETL_HOME);
        if (StringUtils.isBlank(path)) {
            throw new PetlException("The " + ENV_PETL_HOME + " environment variable is required.");
        }
        return path;
    }

    /**
     * @return the File representing the PETL_HOME directory
     */
    public static File getHomeDir() {
        String path = getHomePath();
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new PetlException("The " + ENV_PETL_HOME + " setting of <" + path + ">" + " does not point to a valid directory");
        }
        return dir;
    }

    /**
     * @return the File representing the log file
     */
    public static File getLogFile() {
        File dir = new File(getHomeDir(), "logs");
        if (!dir.exists()) {
            dir.mkdir();
        }
        return new File(dir, "petl.log");
    }
}
