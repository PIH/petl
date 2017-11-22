package org.pih.petl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.logging.LogLevel;
import org.pih.petl.api.JobRunner;
import org.pih.petl.api.config.Config;
import org.pih.petl.api.config.SourceEnvironment;
import org.pih.petl.api.config.TargetEnvironment;
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

        // Normally for a web application, we would run above, and that's it
        // For now, we want this application to start up, run a job, and then exit
        // We determine what job to run, and what LogLevel to use from the arguments passed in
        // Arg1:  The absolute path of the kjb file to run
        // Arg2:  The LogLevel to use [NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL]
        // Arg3:  The name of the source to connect to (if only 1 source is defined, can be left out)

        try {
            String jobPath = args[0];
            LogLevel logLevel = args.length > 1 ? LogLevel.valueOf(args[1]) : LogLevel.MINIMAL;
            String sourceEnvironmentName = args.length > 2 ? args[2] : null;

            Application app = context.getBean(Application.class);
            TargetEnvironment target = app.getConfig().getTargetEnvironment();
            SourceEnvironment source = null;
            for (SourceEnvironment se : app.getConfig().getSourceEnvironments()) {
                if (se.getName().equalsIgnoreCase(sourceEnvironmentName)) {
                    source = se;
                }
            }
            if (source == null) {
                if (sourceEnvironmentName != null) {
                    throw new IllegalArgumentException("Unable to find source environment named " + sourceEnvironmentName);
                }
                if (app.getConfig().getSourceEnvironments().size() == 1) {
                    source = app.getConfig().getSourceEnvironments().get(0);
                }
            }

            JobRunner jobRunner = new JobRunner();
            jobRunner.setJobFilePath(jobPath);
            jobRunner.setLogLevel(logLevel);
            jobRunner.setSourceEnvironment(source);
            jobRunner.setTargetEnvironment(target);
            jobRunner.runJob();
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to execute job", e);
        }
        finally {
            SpringApplication.exit(context);
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
