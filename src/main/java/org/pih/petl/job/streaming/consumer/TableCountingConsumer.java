package org.pih.petl.job.streaming.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.streaming.DebeziumEvent;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableCountingConsumer extends DebeziumConsumer {

    private final Log log = LogFactory.getLog(getClass());

    final Map<String, Integer> numPerTable = new LinkedHashMap<>();

    @Override
    public void accept(DebeziumEvent event) {
        int num = numPerTable.getOrDefault(event.getTable(), 0) + 1;
        numPerTable.put(event.getTable(), num);
        if (num == 1) {
            log.info("First event from " + event.getTable());
            log.info("Event: " + event);
        }
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

    public Map<String, Integer> getNumPerTable() {
        return numPerTable;
    }
}
