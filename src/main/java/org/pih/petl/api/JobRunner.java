package org.pih.petl.api;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.FileLoggingEventListener;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class is responsible for running a Pentaho Job
 */
public class JobRunner {

    //******** PROPERTIES *************
    private String filename;
    private Map<String, String> parameters = new HashMap();
    LogLevel logLevel = LogLevel.DEBUG;

    //******** CONSTRUCTORS ***********

    public JobRunner(String filename) {
        this.filename = filename;
    }

    //******** INSTANCE METHODS *******

    /**
     * Runs this job
     */
    public void runJob() throws Exception {

        System.out.println("***************************************************************************************");
        System.out.println("Initializing the Kettle environment");
        System.out.println("***************************************************************************************\n");
        initializeKettleEnvironment();

        System.out.println("***************************************************************************************");
        System.out.println("Configuring Job");
        System.out.println("***************************************************************************************\n");
        Job job = configureJob();

        System.out.println("***************************************************************************************");
        System.out.println("Running job: " + filename);
        System.out.println("***************************************************************************************\n");

        // Start the job thread, which will execute asynchronously, and wait until it is finished

        job.start();
        job.waitUntilFinished();

        Result result = job.getResult();
        System.out.println("Job completed with result: " + result);

        LoggingBuffer appender = KettleLogStore.getAppender();
        String logText = appender.getBuffer( job.getLogChannelId(), false ).toString();

        System.out.println( "************************************************************************************************" );
        System.out.println( "LOG REPORT: Job generated the following log lines:\n" );
        System.out.println( logText );
        System.out.println( "END OF LOG REPORT" );
        System.out.println( "************************************************************************************************" );
    }

    /**
     * Initialize the Kettle Environment.
     * This isn't a great approach, but it fits with our current kettle setup with Spoon
     * Ideally, all we would need to do would be to call KettleEnvironment.init with some parameters
     * TODO: We should re-work this in a later phase.
     */
    public void initializeKettleEnvironment() {

        // Delete any pre-existing kettle directory
        File kettleDir = new File(Const.getKettleDirectory());
        kettleDir.delete();

        // Recreate the kettle directory
        kettleDir.mkdirs();

        // Add a kettle.properties file
        String pihPentahoHome = parameters.get("PIH_PENTAHO_HOME");
        if (StringUtils.isEmpty(pihPentahoHome)) {
            throw new IllegalArgumentException("Unable to initialize kettle environment.  Parameter PIH_PENTAHO_HOME is required.");
        }

        try {
            Properties kettleProperties = new Properties();
            kettleProperties.put("PIH_PENTAHO_HOME", pihPentahoHome);
            kettleProperties.store(new FileWriter(new File(Const.getKettleDirectory(), "kettle.properties")), null);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to initialize kettle environemnt.  Error writing to kettle.properties.", e);
        }

        try {
            Properties pihKettleProperties = new Properties();
            for (Map.Entry<String, String> e : parameters.entrySet()) {
                pihKettleProperties.put(e.getKey(), e.getValue());
            }
            pihKettleProperties.store(new FileWriter(new File(Const.getKettleDirectory(), "pih-kettle.properties")), null);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to initialize kettle environemnt.  Error writing to pih-kettle.properties.", e);
        }

        // Initialize the Kettle environment
        try {
            KettleEnvironment.init();
        }
        catch (KettleException e) {
            throw new IllegalStateException("Unable to initialize kettle environment.", e);
        }

        // Set up Logger
        String logFileName = "/tmp/petl.log"; // TODO: Replace this
        try {
            if (StringUtils.isNotEmpty(logFileName)) {
                FileLoggingEventListener fileLogListener = new FileLoggingEventListener(logFileName, true);
                KettleLogStore.getAppender().addLoggingEventListener(fileLogListener);
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Error setting up logging to " + logFileName, e);
        }
    }

    /**
     * @return configures a new Job with the given parameters
     */
    public Job configureJob() {
        try {
            JobMeta jobMeta = new JobMeta(filename, null);

            System.out.println("Setting job parameters");
            String[] declaredParameters = jobMeta.listParameters();
            for (int i = 0; i < declaredParameters.length; i++) {
                String parameterName = declaredParameters[i];
                String description = jobMeta.getParameterDescription(parameterName);
                String parameterValue = jobMeta.getParameterDefault(parameterName);
                if (parameters != null && parameters.containsKey(parameterName)) {
                    parameterValue = parameters.get(parameterName);
                }
                System.out.println("Setting parameter " + parameterName + " to " + parameterValue + " [description: " + description + "]");
                jobMeta.setParameterValue(parameterName, parameterValue);
            }

            Job job = new Job(null, jobMeta);
            job.setLogLevel(logLevel);
            return job;
        }
        catch (KettleException e) {
            throw new IllegalStateException("Error configuring Kettle Job.", e);
        }
    }

    //********* PROPERTY ACCESSORS ****************

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(LogLevel logLevel) {
        this.logLevel = logLevel;
    }
}
