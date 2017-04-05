package org.openmrs.contrib.glimpse;

import org.apache.commons.lang.time.StopWatch;
import org.openmrs.contrib.glimpse.api.JobRunner;
import org.openmrs.contrib.glimpse.api.config.Config;
import org.openmrs.contrib.glimpse.api.config.DatabaseConnection;
import org.openmrs.contrib.glimpse.api.config.SourceEnvironment;
import org.openmrs.contrib.glimpse.api.config.TargetEnvironment;
import org.pentaho.di.core.logging.LogLevel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class GlimpseApplication {

    @ConfigurationProperties
    @Bean
    Config getConfig() {
        return new Config();
    }

    /**
     * Run the application
     */
	public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(GlimpseApplication.class, args);

        // Normally for a web application, we would run above, and that's it
        // For now, we want this application to start up, run a job, and then exit
        // We determine what job to run, and what LogLevel to use from the arguments passed in
        // Arg1:  The absolute path of the kjb file to run
        // Arg2:  Optional: The LogLevel to use [NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL] (default is BASIC)

        try {
            String jobPath = args[0];
            LogLevel logLevel = args.length > 1 ? LogLevel.valueOf(args[1]) : LogLevel.BASIC;

            GlimpseApplication app = context.getBean(GlimpseApplication.class);
            TargetEnvironment targetEnvironment = app.getConfig().getTargetEnvironment();
            DatabaseConnection targetDb = targetEnvironment.getDatabaseConnection();
            List<SourceEnvironment> sources = app.getConfig().getSourceEnvironments();

            for (SourceEnvironment source : sources) {
                DatabaseConnection sourceDb = source.getDatabaseConnection();
                Map<String, String> parameters = new HashMap<>();
                parameters.put("pih.country", source.getCountry());
                parameters.put("openmrs.db.host", sourceDb.getHostname());
                parameters.put("openmrs.db.port", sourceDb.getPort().toString());
                parameters.put("openmrs.db.name", sourceDb.getDatabaseName());
                parameters.put("openmrs.db.user", sourceDb.getUsername());
                parameters.put("openmrs.db.password", sourceDb.getPassword());
                parameters.put("warehouse.db.host", targetDb.getHostname());
                parameters.put("warehouse.db.port", targetDb.getPort().toString());
                parameters.put("warehouse.db.name", targetDb.getDatabaseName());
                parameters.put("warehouse.db.user", targetDb.getUsername());
                parameters.put("warehouse.db.password", targetDb.getPassword());
                parameters.put("warehouse.db.key_prefix", source.getKeyPrefix());

                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                System.out.println("Loading " + source.getName() + " into " + targetEnvironment.getDatabaseConnection().getDatabaseName());

                JobRunner jr = new JobRunner(jobPath);
                jr.setParameters(parameters);
                jr.setLogLevel(logLevel);
                jr.runJob();

                stopWatch.stop();
                System.out.println("Job executed in:  " + stopWatch.toString());
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to execute job", e);
        }
        finally {
            SpringApplication.exit(context);
        }
	}
}
