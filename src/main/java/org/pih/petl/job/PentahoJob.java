package org.pih.petl.job;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * This class is responsible for running a Pentaho Kettle PetlJob
 */
@Component("pentaho-job")
public class PentahoJob extends AbstractJob {

    private static final Log log = LogFactory.getLog(PentahoJob.class);

    //******** CONSTANT CONFIGURATION PARAMETERS *******

    public static final String JOB_FILE_PATH = "job.filePath";
    public static final String JOB_LOG_LEVEL = "job.logLevel";
    public static final String PIH_PENTAHO_HOME = "pih.pentahoHome";

    @Autowired
    public PentahoJob(ApplicationConfig applicationConfig) {
        super(applicationConfig);
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {

        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());
        Properties configuration = configReader.getAsProperties();

        String jobFilePath = configuration.getProperty(JOB_FILE_PATH);
        log.debug("PetlJob file path: " + jobFilePath);
        File jobFile = applicationConfig.getJobConfigFile(jobFilePath).getConfigFile();

        /*
        First, we want to create an execution environment for this job to set up Kettle with appropriate properties
        We create a subdirectory under work/{{jobId}}/.kettle, and set KETTLE_HOME to point to this.
        This gives us somewhere for this execution, where we can write kettle.properties and pih-kettle.properties,
        so that the pipeline can find and use these as currently designed.
        */
        File workDir = ensureDir(applicationConfig.getPetlHomeDir(), "work");
        File jobDir = ensureDir(workDir, jobExecution.getUuid());
        File kettleDir = ensureDir(jobDir, ".kettle");
        System.setProperty("KETTLE_HOME", jobDir.getAbsolutePath());

        try {
            // Configure kettle.properties with PIH_PENTAHO_HOME which we acquire from the configuration
            File srcDir = new File(configuration.getProperty(PIH_PENTAHO_HOME, jobFile.getParent()));
            if (!srcDir.exists()) {
                throw new PetlException("Unable to initialize kettle environment.  Unable to find: " + srcDir);
            }
            try {
                Properties kettleProperties = new Properties();
                kettleProperties.put("PIH_PENTAHO_HOME", srcDir.getAbsolutePath());
                kettleProperties.store(new FileWriter(new File(kettleDir, "kettle.properties")), null);
                log.debug("Wrote kettle.properties to " + kettleDir);
            }
            catch (IOException e) {
                throw new PetlException("Unable to initialize kettle.  Error writing to kettle.properties.", e);
            }

            // Initialize the Kettle environment
            try {
                log.debug("Initializing Kettle Environment");
                log.debug("KETTLE_HOME = " + System.getProperty("KETTLE_HOME"));
                List<String> plugins = configReader.getStringList("job", "plugins");
                if (!plugins.contains("org.pentaho.di.trans.steps.append.AppendMeta")) {
                    plugins.add("org.pentaho.di.trans.steps.append.AppendMeta");
                }
                String kettlePluginClasses = String.join(",", plugins);
                System.setProperty("KETTLE_PLUGIN_CLASSES", kettlePluginClasses);
                log.debug("KETTLE_PLUGIN_CLASSES: " + System.getProperty("KETTLE_PLUGIN_CLASSES"));
                KettleEnvironment.init();
            }
            catch (KettleException e) {
                throw new PetlException("Unable to initialize kettle environment.", e);
            }

            log.debug("CONFIGURATION:");

            Set<String> propertyNames = configuration.stringPropertyNames();
            for (String property : propertyNames) {
                String propertyValue = configuration.getProperty(property);
                if (property.toLowerCase().contains("password")) {
                    propertyValue = "************";
                }
                log.debug(property + " = " + propertyValue);
            }

            // Write passed configuration properties to pih-kettle.properties

            try {
                configuration.store(new FileWriter(new File(kettleDir, "pih-kettle.properties")), null);
                log.debug("Wrote pih-kettle.properties to " + kettleDir);
            }
            catch (IOException e) {
                throw new PetlException("Unable to initialize kettle.  Error writing to pih-kettle.properties.", e);
            }

            // Load job path, log level, and set named parameters based on configuration properties

            JobMeta jobMeta = new JobMeta(jobFile.getAbsolutePath(), null);

            log.debug("Petl Job parameters: ");
            String[] declaredParameters = jobMeta.listParameters();
            for (int i = 0; i < declaredParameters.length; i++) {
                String parameterName = declaredParameters[i];
                String parameterValue = jobMeta.getParameterDefault(parameterName);
                if (configuration.containsKey(parameterName)) {
                    parameterValue = configuration.getProperty(parameterName);
                }
                log.debug(parameterName + " -> " + parameterValue);
                jobMeta.setParameterValue(parameterName, parameterValue);
            }

            String logLevelConfig = configuration.getProperty(JOB_LOG_LEVEL, "MINIMAL");
            LogLevel logLevel = LogLevel.valueOf(logLevelConfig);
            log.debug("PetlJob log level: " + logLevel);

            Job job = new Job(null, jobMeta);
            job.setLogLevel(logLevel);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            log.debug("Starting PetlJob Execution...");

            job.start();  // Start the job thread, which will execute asynchronously
            job.waitUntilFinished(); // Wait until the job thread is finished

            stopWatch.stop();

            Result result = job.getResult();

            log.debug("***************");
            log.debug("PetlJob executed in:  " + stopWatch);
            log.debug("PetlJob Result: " + result);
            log.debug("***************");

            if (result.getNrErrors() > 0 || result.getExitStatus() > 0) {
                String msg = result.getLogText();
                if (StringUtils.isEmpty(msg)) {
                    msg = "There was an error during Pentaho job execution.  Num Errors: " + result.getNrErrors() + "; exit code: " + result.getExitStatus();
                }
                throw new PetlException(msg);
            }
        }
        finally {
            try {
                FileUtils.deleteDirectory(jobDir);
            }
            catch (Exception e) {
                log.warn("Error deleting job directory: " + jobDir);
            }
            KettleEnvironment.reset();
        }
    }

    public File ensureDir(File parent, String dirName) {
        File f = new File(parent, dirName);
        f.mkdirs();
        return f;
    }
}
