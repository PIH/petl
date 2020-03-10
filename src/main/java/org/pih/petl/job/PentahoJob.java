package org.pih.petl.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.FileLoggingEventListener;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.JobMeta;
import org.pih.petl.PetlException;
import org.pih.petl.api.EtlStatus;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

/**
 * This class is responsible for running a Pentaho Kettle PetlJob
 */
public class PentahoJob implements PetlJob {

    private static final Log log = LogFactory.getLog(PentahoJob.class);
    private static boolean refreshInProgress = false;

    //******** CONSTANT CONFIGURATION PARAMETERS *******

    public static final String JOB_FILE_PATH = "job.filePath";
    public static final String JOB_LOG_LEVEL = "job.logLevel";

    private EtlService etlService;
    private JobConfigReader configReader;
    private File configFile;
    private JobConfig config;

    /**
     * Creates a new instance of the job with the given configuration path
     */
    public PentahoJob(EtlService etlService, JobConfigReader configReader, String configPath) {
        this.etlService = etlService;
        this.configReader = configReader;
        this.configFile = configReader.getConfigFile(configPath);
        this.config = configReader.getEtlJobConfigFromFile(this.configFile);
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute() {

        if (!refreshInProgress) {
            refreshInProgress = true;
            try {
                Properties configuration = config.getAsProperties();

                // TODO: Add validation in
                String jobFilePath = configuration.getProperty(JOB_FILE_PATH);
                log.info("PetlJob file path: " + jobFilePath);
                File jobFile = configReader.getConfigFile(jobFilePath);

                // Initialize the status table with this job execution
                EtlStatus etlStatus = etlService.createStatus(jobFilePath);

                /*
                First, we want to create an execution environment for this job to set up Kettle with appropriate properties
                TODO:  Consider removing this in later phases, but we will need to change the pipeline code accordingly.
                We create a subdirectory under work/{{jobId}}/.kettle, and set KETTLE_HOME to point to this.
                This gives us somewhere for this execution, where we can write kettle.properties and pih-kettle.properties,
                so that the pipeline can find and use these as currently designed.
                */
                File workDir = ensureDir(configReader.getApplicationConfig().getHomeDir(), "work");
                File jobDir = ensureDir(workDir, etlStatus.getUuid());
                File kettleDir = ensureDir(jobDir, ".kettle");
                System.setProperty("KETTLE_HOME", jobDir.getAbsolutePath());

                try {
                    // Configure kettle.properties with PIH_PENTAHO_HOME which we aquire from the configuration
                    File srcDir = configReader.getApplicationConfig().getHomeDir();
                    if (!srcDir.exists()) {
                        throw new PetlException("Unable to initialize kettle environemnt.  Unable to find: " + srcDir);
                    }
                    try {
                        Properties kettleProperties = new Properties();
                        kettleProperties.put("PIH_PENTAHO_HOME", srcDir.getAbsolutePath());
                        kettleProperties.store(new FileWriter(new File(kettleDir, "kettle.properties")), null);
                        log.info("Wrote kettle.properties to " + kettleDir);
                    }
                    catch (IOException e) {
                        throw new PetlException("Unable to initialize kettle.  Error writing to kettle.properties.", e);
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

                    log.info("CONFIGURATION:");

                    Set<String> propertyNames = configuration.stringPropertyNames();
                    for (String property : propertyNames) {
                        String propertyValue = configuration.getProperty(property);
                        if (property.toLowerCase().contains("password")) {
                            propertyValue = "************";
                        }
                        log.info(property + " = " + propertyValue);
                    }

                    // Write passed configuration properties to pih-kettle.properties

                    try {
                        configuration.store(new FileWriter(new File(kettleDir, "pih-kettle.properties")), null);
                        log.info("Wrote pih-kettle.properties to " + kettleDir);
                    }
                    catch (IOException e) {
                        throw new PetlException("Unable to initialize kettle.  Error writing to pih-kettle.properties.", e);
                    }

                    // Load job path, log level, and set named parameters based on configuration properties

                    JobMeta jobMeta = new JobMeta(jobFile.getAbsolutePath(), null);

                    log.info("PetlJob parameters: ");
                    String[] declaredParameters = jobMeta.listParameters();
                    for (int i = 0; i < declaredParameters.length; i++) {
                        String parameterName = declaredParameters[i];
                        String parameterValue = jobMeta.getParameterDefault(parameterName);
                        if (configuration.containsKey(parameterName)) {
                            parameterValue = configuration.getProperty(parameterName);
                        }
                        log.info(parameterName + " -> " + parameterValue);
                        jobMeta.setParameterValue(parameterName, parameterValue);
                    }

                    String logLevelConfig = configuration.getProperty(JOB_LOG_LEVEL, "MINIMAL");
                    LogLevel logLevel = LogLevel.valueOf(logLevelConfig);
                    log.info("PetlJob log level: " + logLevel);

                    org.pentaho.di.job.Job job = new org.pentaho.di.job.Job(null, jobMeta);
                    job.setLogLevel(logLevel);

                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();

                    log.info("Starting PetlJob Execution...");

                    FileLoggingEventListener logger = setupLogger(job);
                    job.start();  // Start the job thread, which will execute asynchronously
                    job.waitUntilFinished(); // Wait until the job thread is finished
                    shutdownLogger(logger);

                    stopWatch.stop();

                    Result result = job.getResult();
                    // TODO: We can inspect the result here and take action (eg. notify on errors, etc)

                    log.info("***************");
                    log.info("PetlJob executed in:  " + stopWatch.toString());
                    log.info("PetlJob Result: " + result);
                    log.info("***************");
                }
                catch (Exception e) {
                    etlStatus.setStatus("Import Failed");
                    etlStatus.setCompleted(new Date());
                    etlStatus.setErrorMessage(e.getMessage());
                    etlService.updateEtlStatus(etlStatus);
                    log.error("PETL Job Failed", e);
                    throw new PetlException(e);
                }
                finally {
                    try {
                        FileUtils.deleteDirectory(jobDir);
                    }
                    catch (Exception e) {
                        log.warn("Error deleting job directory: " + jobDir);
                    }
                }
            }
            finally {
                refreshInProgress = false;
            }
        }
    }

    public FileLoggingEventListener setupLogger(org.pentaho.di.job.Job job) {
        File logFile = configReader.getApplicationConfig().getLogFile();
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
}
