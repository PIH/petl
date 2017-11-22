package org.pih.petl.api;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.FileLoggingEventListener;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pih.petl.Application;
import org.pih.petl.PetlException;
import org.pih.petl.api.config.DatabaseConnection;
import org.pih.petl.api.config.SourceEnvironment;
import org.pih.petl.api.config.TargetEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * This class is responsible for running a Kettle Job
 */
public class JobRunner {

    private static final Log log = LogFactory.getLog(JobRunner.class);

    //******** PROPERTIES *************
    private String jobId = UUID.randomUUID().toString();
    private String jobFilePath;
    private SourceEnvironment sourceEnvironment;
    private TargetEnvironment targetEnvironment;
    private LogLevel logLevel = LogLevel.BASIC;

    //******** CONSTRUCTORS ***********

    public JobRunner() {
    }

    //******** INSTANCE METHODS *******

    /**
     * Runs this job
     */
    public void runJob() throws Exception {

        log.info("***************");
        log.info("Running Job: " + jobId);
        log.info("***************");

        /*
            First, we want to create an execution environment for this job in order to set up Kettle with appropriate properties
            The plan would be to remove the need for this in later phases, but we will need to change the pipeline code accordingly.  TODO
            We create a subdirectory under work/{{jobId}}/.kettle, and set KETTLE_HOME to point to this.
            This gives us somewhere, specific to this execution, where we can write kettle.properties and pih-kettle.properties,
            so that the pipeline can find and use these as currently designed.
         */

        File workDir = ensureDir(Application.getHomeDir(), "work");
        File jobDir = ensureDir(workDir, jobId);
        File kettleDir = ensureDir(jobDir, ".kettle");
        System.setProperty("KETTLE_HOME", jobDir.getAbsolutePath());

        try {
            // First, we configure kettle.properties to contain the setting for PIH_PENTAHO_HOME, which we aquire from an environment variable

            String pihPentahoHome = System.getenv("PIH_PENTAHO_HOME");
            if (StringUtils.isEmpty(pihPentahoHome)) {
                throw new PetlException("Unable to find environment variable PIH_PENTAHO_HOME.");
            }
            File srcDir = new File(pihPentahoHome);
            if (!srcDir.exists()) {
                throw new PetlException("Unable to initialize kettle environemnt.  Unable to find src directory at: " + srcDir);
            }
            try {
                Properties kettleProperties = new Properties();
                kettleProperties.put("PIH_PENTAHO_HOME", srcDir.getAbsolutePath());
                kettleProperties.store(new FileWriter(new File(kettleDir, "kettle.properties")), null);
                log.info("Wrote kettle.properties to " + kettleDir);
            }
            catch (IOException e) {
                throw new PetlException("Unable to initialize kettle environemnt.  Error writing to kettle.properties.", e);
            }

            // Initialize the Kettle environment
            try {
                log.info("Initializing Kettle Environment");
                log.info("KETTLE_HOME = " + System.getProperty("KETTLE_HOME"));
                KettleEnvironment.init();
            }
            catch (KettleException e) {
                throw new PetlException("Unable to initialize kettle environment.", e);
            }

            Properties p = new Properties();
            try {
                if (getSourceEnvironment() != null) {
                    DatabaseConnection sourceDb = getSourceEnvironment().getDatabaseConnection();
                    p.put("pih.country", getSourceEnvironment().getCountry());
                    p.put("openmrs.db.host", sourceDb.getHostname());
                    p.put("openmrs.db.port", sourceDb.getPort().toString());
                    p.put("openmrs.db.name", sourceDb.getDatabaseName());
                    p.put("openmrs.db.user", sourceDb.getUsername());
                    p.put("openmrs.db.password", sourceDb.getPassword());
                    p.put("warehouse.db.key_prefix", getSourceEnvironment().getKeyPrefix());
                }
                if (getTargetEnvironment() != null) {
                    DatabaseConnection targetDb = getTargetEnvironment().getDatabaseConnection();
                    p.put("warehouse.db.host", targetDb.getHostname());
                    p.put("warehouse.db.port", targetDb.getPort().toString());
                    p.put("warehouse.db.name", targetDb.getDatabaseName());
                    p.put("warehouse.db.user", targetDb.getUsername());
                    p.put("warehouse.db.password", targetDb.getPassword());
                }
                p.store(new FileWriter(new File(kettleDir, "pih-kettle.properties")), null);
                log.info("Wrote pih-kettle.properties to " + kettleDir);
            }
            catch (IOException e) {
                throw new PetlException("Unable to initialize kettle environemnt.  Error writing to pih-kettle.properties.", e);
            }

            JobMeta jobMeta = new JobMeta(getJobFilePath(), null);

            log.info("Setting job parameters");
            String[] declaredParameters = jobMeta.listParameters();
            for (int i = 0; i < declaredParameters.length; i++) {
                String parameterName = declaredParameters[i];
                String description = jobMeta.getParameterDescription(parameterName);
                String parameterValue = jobMeta.getParameterDefault(parameterName);
                if (p.containsKey(parameterName)) {
                    parameterValue = p.getProperty(parameterName);
                }
                log.info("Setting parameter " + parameterName + " to " + parameterValue + " [description: " + description + "]");
                jobMeta.setParameterValue(parameterName, parameterValue);
            }

            Job job = new Job(null, jobMeta);
            job.setLogLevel(logLevel);
            log.info("Job file: " + jobFilePath);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            log.info("Starting Job Execution: " + sourceEnvironment.getName() + " -> " + targetEnvironment.getDatabaseConnection().getDatabaseName());

            FileLoggingEventListener logger = setupLogger(job);
            job.start();  // Start the job thread, which will execute asynchronously
            job.waitUntilFinished(); // Wait until the job thread is finished
            shutdownLogger(logger);

            stopWatch.stop();

            Result result = job.getResult();  // TODO: We can inspect the result here and take action (eg. notify on errors, etc)

            log.info("***************");
            log.info("Job executed in:  " + stopWatch.toString());
            log.info("Job Result: " + result);
            log.info("***************");
        }
        finally {
            FileUtils.deleteDirectory(jobDir);
        }
    }

    public FileLoggingEventListener setupLogger(Job job) {
        File logFile = Application.getLogFile();
        try {
            FileLoggingEventListener fileLogListener = new FileLoggingEventListener(job.getLogChannelId(), logFile.getAbsolutePath(), true);
            KettleLogStore.getAppender().addLoggingEventListener(fileLogListener);
            return fileLogListener;
        }
        catch (Exception e) {
            throw new PetlException("Error setting up logging to " + logFile, e);
        }
    }

    public void shutdownLogger(FileLoggingEventListener loggingEventListener) {
        try {
            KettleLogStore.getAppender().removeLoggingEventListener(loggingEventListener);
            loggingEventListener.close();
        }
        catch (KettleException e) {
            throw new PetlException("Unable to shutdown logger", e);
        }
    }

    public File ensureDir(File parent, String dirName) {
        File f = new File(parent, dirName);
        f.mkdirs();
        return f;
    }

    //********* PROPERTY ACCESSORS ****************

    public String getJobId() {
        return jobId;
    }

    public String getJobFilePath() {
        return jobFilePath;
    }

    public void setJobFilePath(String jobFilePath) {
        this.jobFilePath = jobFilePath;
    }

    public SourceEnvironment getSourceEnvironment() {
        return sourceEnvironment;
    }

    public void setSourceEnvironment(SourceEnvironment sourceEnvironment) {
        this.sourceEnvironment = sourceEnvironment;
    }

    public TargetEnvironment getTargetEnvironment() {
        return targetEnvironment;
    }

    public void setTargetEnvironment(TargetEnvironment targetEnvironment) {
        this.targetEnvironment = targetEnvironment;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(LogLevel logLevel) {
        this.logLevel = logLevel;
    }
}
