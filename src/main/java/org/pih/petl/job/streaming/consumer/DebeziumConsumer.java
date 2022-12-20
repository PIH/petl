package org.pih.petl.job.streaming.consumer;

import io.debezium.engine.ChangeEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.streaming.DebeziumEvent;

import java.util.function.Consumer;

public abstract class DebeziumConsumer implements Consumer<ChangeEvent<String, String>> {

    private final Log log = LogFactory.getLog(getClass());

    long firstProcessTime = 0;
    long numRecordsProcessed = 0;
    long lastProcessTime = 0;

    @Override
    public final void accept(ChangeEvent<String, String> changeEvent) {
        if (firstProcessTime == 0) {
            firstProcessTime = System.currentTimeMillis();
        }
        numRecordsProcessed++;
        lastProcessTime = System.currentTimeMillis();
        accept(new DebeziumEvent(changeEvent));
    }

    /**
     * Called once before any events are processed
     */
    public void startup() {
        log.info("Starting up Debezium Consumer " + getClass().getSimpleName());
    }

    public abstract void accept(DebeziumEvent event);

    /**
     * Called once prior at shutdown
     */
    public void shutdown() {
        log.info("Shutting down Debezium Consumer " + getClass().getSimpleName());
    }

    public long getFirstProcessTime() {
        return firstProcessTime;
    }

    public void setFirstProcessTime(long firstProcessTime) {
        this.firstProcessTime = firstProcessTime;
    }

    public long getNumRecordsProcessed() {
        return numRecordsProcessed;
    }

    public void setNumRecordsProcessed(long numRecordsProcessed) {
        this.numRecordsProcessed = numRecordsProcessed;
    }

    public long getLastProcessTime() {
        return lastProcessTime;
    }

    public void setLastProcessTime(long lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }
}
