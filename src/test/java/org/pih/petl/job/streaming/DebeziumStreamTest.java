package org.pih.petl.job.streaming;

import io.debezium.engine.ChangeEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.DataSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DebeziumStreamTest {

    private static final Log log = LogFactory.getLog(DebeziumStreamTest.class);

    String schema = "humci";
    String[] tables = {"person", "patient", "patient_identifier", "person_name", "person_address", "encounter", "obs" };

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void testDebezium() throws Exception {
        Integer streamId = 100002;

        DataSource dataSource = new DataSource();
        dataSource.setDatabaseType("mysql");
        dataSource.setHost("localhost");
        dataSource.setPort("3308");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setDatabaseName(schema);

        DebeziumStream stream = new DebeziumStream(streamId, dataSource, Arrays.asList(tables), new File("/tmp"));

        stream.reset();

        DebeziumConsumer consumer = new DebeziumConsumer();

        stream.start(consumer);
        Thread.sleep(5000);
        while (consumer.lastProcessTime != 0 && System.currentTimeMillis() - consumer.lastProcessTime < 1000) {
            Thread.sleep(1000);
            log.info("Number of records processed: " + consumer.numRecordsProcessed);
        }
        stream.stop();

        int processTime = (int)(consumer.lastProcessTime - consumer.firstProcessTime)/1000;

        log.info("Changelog read.  Num processed: " + consumer.numRecordsProcessed + "; Total time: " + processTime + "s");
        for (String table : consumer.numPerTable.keySet()) {
            log.info(table + ": " + consumer.numPerTable.get(table) + " rows processed");
        }
    }

    public static class DebeziumConsumer implements Consumer<ChangeEvent<String, String>> {

        long firstProcessTime = 0;
        long numRecordsProcessed;
        long lastProcessTime = 0;
        final Map<String, Integer> numPerTable = new LinkedHashMap<>();

        @Override
        public void accept(ChangeEvent<String, String> changeEvent) {
            if (firstProcessTime == 0) {
                firstProcessTime = System.currentTimeMillis();
            }
            numRecordsProcessed++;
            lastProcessTime = System.currentTimeMillis();
            DebeziumEvent event = new DebeziumEvent(changeEvent);
            int num = numPerTable.getOrDefault(event.getTable(), 0) + 1;
            numPerTable.put(event.getTable(), num);
            if (num == 1) {
                log.info("First event from " + event.getTable());
                log.info("Key: " + changeEvent.key());
                log.info("Value: " + changeEvent.value());
                log.info("Destination: " + changeEvent.destination());
            }
        }
    }
}
