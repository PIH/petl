package org.pih.petl.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.JobExecution;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfigReader;
import org.pih.petl.job.streaming.DebeziumStream;
import org.pih.petl.job.streaming.consumer.DebeziumConsumer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for running a job against a Debezium stream
 * <p>
 * Required configuration:
 * streamId:   unique, numeric id that associates this particular Debezium job with mysql as a replication node
 * datasource: a mysql datasource that Debezium.  the user and data source must be able to read the row-level binlog
 * tables:  the list of tables to read from the binlog
 * functionClass:  the class that implements Consumer<ChangeEvent<String, String>> that should be executed for each record
 * <p>
 * Optional configuration:
 * resetEachExecution:  if true, this will start the job over from scratch, rather than picking up from last offset
 * debezium:  any nested property within a "debezium" configuration node will override any default value
 */
public abstract class DebeziumJob extends AbstractJob {

    private final Log log = LogFactory.getLog(getClass());

    public DebeziumJob(ApplicationConfig applicationConfig) {
        super(applicationConfig);
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {
        log.debug("Executing " + getClass().getSimpleName());
        JobConfigReader configReader = new JobConfigReader(applicationConfig, jobExecution.getJobConfig());
        int streamId = getStreamId(configReader);
        DataSource dataSource = getDataSource(configReader);
        List<String> tables = getTables(dataSource, configReader);
        File dataDir = getDataDirectory(applicationConfig);

        DebeziumStream stream = new DebeziumStream(streamId, dataSource, tables, dataDir);

        // Allow explicit overrides or additions to above defaults
        Map<String, String> debeziumConfig = configReader.getMap("debezium");
        for (String key : debeziumConfig.keySet()) {
            stream.setProperty(key, debeziumConfig.get(key));
        }

        DebeziumConsumer function = getConsumer(configReader);

        // Reset the stream is configured to do so
        if (configReader.getBoolean(false, "resetEachExecution")) {
            stream.reset();
        }

        stream.start(function);
    }

    /**
     * @return the serverId to use from the configuration
     */
    public int getStreamId(JobConfigReader configReader) {
        try {
            return Integer.parseInt(configReader.getString("streamId"));
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Please specify a unique, numeric streamId for this job");
        }
    }

    /**
     * @return the datasource to use from the configuration
     */
    public DataSource getDataSource(JobConfigReader configReader) {
        DataSource dataSource = configReader.getDataSource("datasource");
        if (dataSource == null || !dataSource.getDatabaseType().equals("mysql")) {
            throw new IllegalArgumentException("Please specify a MySQL datasource for this job");
        }
        return dataSource;
    }

    /**
     * @return the tables to monitor from the configuration
     */
    public List<String> getTables(DataSource dataSource, JobConfigReader configReader) {
        List<String> tables = new ArrayList<>();
        for (String table : configReader.getStringList("tables")) {
            tables.add(dataSource.getDatabaseName() + "." + table);
        }
        return tables;
    }

    /**
     * @return the data directory from the configuration
     */
    public File getDataDirectory(ApplicationConfig applicationConfig) {
        File dataDir = new File(applicationConfig.getPetlHomeDir(), "work");
        return new File(dataDir, "debezium");
    }

    /**
     * @return the function to run for each change event
     */
    @SuppressWarnings("unchecked")
    public DebeziumConsumer getConsumer(JobConfigReader configReader) {
        try {
            String functionClassName = configReader.getString("functionClass");
            Class<?> functionClass = DebeziumJob.class.getClassLoader().loadClass(functionClassName);
            Object functionObject = functionClass.getConstructor().newInstance();
            return (DebeziumConsumer) functionObject;
        }
        catch (Exception e) {
            throw new PetlException("Unable to create change event function", e);
        }
    }
}
