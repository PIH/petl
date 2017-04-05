/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.openmrs.contrib.glimpse.api;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.io.File;
import java.io.FileWriter;
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

        // Initialize the Kettle environment
        KettleEnvironment.init();

        // TODO: This isn't a great approach, but it fits with our current kettle setup with Spoon, so working with that for step 1

        System.out.println("Configuring kettle.properties");
        Properties kettleProperties = new Properties();
        kettleProperties.put("PIH_PENTAHO_HOME", "/home/mseaton/code/pih-pentaho");
        kettleProperties.store(new FileWriter(new File(Const.getKettleDirectory(), "kettle.properties")), null);

        System.out.println("Configuring pih-kettle.properties");
        Properties pihKettleProperties = new Properties();
        for (Map.Entry<String, String> e : parameters.entrySet()) {
            pihKettleProperties.put(e.getKey(), e.getValue());
        }
        pihKettleProperties.store(new FileWriter(new File(Const.getKettleDirectory(), "pih-kettle.properties")), null);

        System.out.println("***************************************************************************************");
        System.out.println("Running job: " + filename);
        System.out.println("***************************************************************************************\n");

        JobMeta jobMeta = new JobMeta( filename, null );

        System.out.println( "Setting job parameters" );
        String[] declaredParameters = jobMeta.listParameters();
        for (int i=0; i<declaredParameters.length; i++) {
            String parameterName = declaredParameters[i];
            String description = jobMeta.getParameterDescription(parameterName);
            String parameterValue = jobMeta.getParameterDefault(parameterName);
            if (parameters != null && parameters.containsKey(parameterName)) {
                parameterValue = parameters.get(parameterName);
            }
            System.out.println( "Setting parameter " + parameterName + " to " + parameterValue + " [description: " + description + "]" );
            jobMeta.setParameterValue(parameterName, parameterValue);
        }

        Job job = new Job( null, jobMeta );
        job.setLogLevel( logLevel );

        System.out.println( "Starting job" );

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
