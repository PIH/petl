package org.pih.petl.job.streaming;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.streaming.consumer.CassandraConsumer;
import org.pih.petl.job.streaming.consumer.DebeziumConsumer;
import org.pih.petl.job.streaming.consumer.TableCountingConsumer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.Arrays;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DebeziumStreamTest {

    private static final Log log = LogFactory.getLog(DebeziumStreamTest.class);

    String schema = "humci";

    // The order here matters.  We need to make sure that tables are in the order in which they would have originally been inserted for a given patient
    String[] tables = {"person", "patient", "patient_identifier", "person_name", "person_address", "encounter", "obs" };

    static {
        SpringRunnerTest.setupEnvironment();
    }

    public DebeziumStream getStream(Integer streamId) {
        DataSource dataSource = new DataSource();
        dataSource.setDatabaseType("mysql");
        dataSource.setHost("localhost");
        dataSource.setPort("3308");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setDatabaseName(schema);
        return new DebeziumStream(streamId, dataSource, Arrays.asList(tables), new File("/tmp"));
    }

    protected void executeStream(DebeziumStream stream, DebeziumConsumer consumer) throws Exception {
        stream.reset();
        stream.start(consumer);
        Thread.sleep(5000);
        while (consumer.getLastProcessTime() != 0 && System.currentTimeMillis() - consumer.getLastProcessTime() < 1000) {
            Thread.sleep(1000);
            log.info("Number of records processed: " + consumer.getNumRecordsProcessed());
        }
        stream.stop(consumer);

        int processTime = (int)(consumer.getLastProcessTime() - consumer.getFirstProcessTime())/1000;

        log.info("Changelog read.  Num: " + consumer.getNumRecordsProcessed() + "; Time: " + processTime + "s");
    }

    @Test
    public void testTableCountingConsumer() throws Exception {
        executeStream(getStream(100002), new TableCountingConsumer());
    }

    @Test
    public void testCassandraConsumer() throws Exception {
        executeStream(getStream(100005), new CassandraConsumer());
    }
}
