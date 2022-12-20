package org.pih.petl.job.streaming;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.streaming.consumer.DebeziumConsumer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This stream emits change events from a configured database
 */
public class DebeziumStream {

    private final Log log = LogFactory.getLog(getClass());

    private final Integer streamId;
    private final DataSource dataSource;
    private final List<String> tables;
    private final Properties config;
    private final String streamName;
    private final File jobDataDir;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    /**
     * Instantiates a new stream with the given streamId, dataSource, tables, and dataDir
     */
    public DebeziumStream(Integer streamId, DataSource dataSource, List<String> tables, File dataDir) {
        this.streamId = streamId;
        this.dataSource = dataSource;
        this.tables = tables;
        this.config = new Properties();
        this.streamName = dataSource.getDatabaseName() + "_" + streamId;
        this.jobDataDir = new File(dataDir, streamName);

        config.put("name", streamName + "_connector");
        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        config.put("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        config.put("offset.storage.file.filename", new File(jobDataDir, "offsets.dat").getAbsolutePath());
        config.put("offset.flush.interval.ms", "60000");
        config.put("database.hostname", dataSource.getHost());
        config.put("database.port", dataSource.getPort());
        config.put("database.user", dataSource.getUser());
        config.put("database.password", dataSource.getPassword());
        config.put("database.dbname", dataSource.getDatabaseName());
        config.put("database.include.list", dataSource.getDatabaseName());
        if (tables != null && !tables.isEmpty()) {
            String tablePrefix = dataSource.getDatabaseName() + ".";
            String tableConfig = tables.stream().map(t -> tablePrefix + t).collect(Collectors.joining(","));
            config.put("table.include.list", tableConfig);
        }
        config.put("include.schema.changes", "false");
        config.put("database.server.id", "100002");
        config.put("database.server.name", streamName);
        config.put("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        config.put("database.history.file.filename", new File(jobDataDir, "schema_history.dat").getAbsolutePath());
        config.put("decimal.handling.mode", "double");
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("key.converter.schemas.enable", "false");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");
    }

    /**
     * Adds or overrides a particular configuration property for Debezium.
     * Must be called before the stream is started
     */
    public void setProperty(String key, String value) {
        config.setProperty(key, value);
    }

    /**
     * @return the currently configured offsets file
     */
    public File getOffsetsFile() {
        return new File(config.getProperty("offset.storage.file.filename"));
    }

    /**
     * @return the currently configured database schema history file
     */
    public File getDatabaseHistoryFile() {
        return new File(config.getProperty("database.history.file.filename"));
    }

    /**
     * Allows for resetting the stream.  This deletes any existing history and offset files.
     */
    public void reset() {
        log.info("Resetting Debezium Stream: " + streamName);
        FileUtils.deleteQuietly(getOffsetsFile());
        FileUtils.deleteQuietly(getDatabaseHistoryFile());
    }

    /**
     * Starts the debezium stream
     */
    public void start(DebeziumConsumer debeziumConsumer) {
        log.info("Starting Debezium Stream: " + streamName);
        log.debug("Configuration: " + config);

        if (getOffsetsFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + getOffsetsFile().getParentFile());
        }
        if (getDatabaseHistoryFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + getOffsetsFile().getParentFile());
        }

        debeziumConsumer.startup();

        engine = DebeziumEngine.create(Json.class)
                .using(config)
                .notifying(debeziumConsumer)
                .build();

        executor.execute(engine);
    }

    public void stop(DebeziumConsumer debeziumConsumer) {
        log.info("Stopping Debezium Stream: " + streamName);
        try {
            debeziumConsumer.shutdown();
        }
        catch (Exception e) {
            log.warn("An error occurred while trying to shutdown the consumer", e);
        }
        try {
            if (engine != null) {
                engine.close();
            }
        }
        catch (IOException e) {
            log.warn("An error occurred while attempting to close the engine", e);
        }
        try {
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("Waiting another 5 seconds for the embedded engine to shut down");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Integer getStreamId() {
        return streamId;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public List<String> getTables() {
        return tables;
    }

    public Properties getConfig() {
        return config;
    }

    public String getStreamName() {
        return streamName;
    }

    public File getJobDataDir() {
        return jobDataDir;
    }
}
