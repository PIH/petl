package org.pih.petl.job.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.debezium.engine.ChangeEvent;

import java.io.Serializable;

/**
 * Represents a Debezium Change Event
 */
public class DebeziumEvent implements Serializable {

    private static final JsonMapper mapper = new JsonMapper();

    private final ChangeEvent<String, String> changeEvent;
    private final Long timestamp;
    private final DebeziumOperation operation;
    private final ObjectMap key;
    private final ObjectMap before;
    private final ObjectMap after;
    private final ObjectMap source;

    public DebeziumEvent(ChangeEvent<String, String> changeEvent) {
        this.changeEvent = changeEvent;
        try {
            JsonNode keyNode = mapper.readTree(changeEvent.key());
            JsonNode valueNode = mapper.readTree(changeEvent.value());
            timestamp = valueNode.get("ts_ms").longValue();
            operation = DebeziumOperation.parse(valueNode.get("op").textValue());
            key = mapper.treeToValue(keyNode, ObjectMap.class);
            before = mapper.treeToValue(valueNode.get("before"), ObjectMap.class);
            after = mapper.treeToValue(valueNode.get("after"), ObjectMap.class);
            source = mapper.treeToValue(valueNode.get("source"), ObjectMap.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ChangeEvent<String, String> getChangeEvent() {
        return changeEvent;
    }

    public String getServerName() {
        return source.getString("name");
    }

    public String getTable() {
        return source.getString("table");
    }

    public DebeziumOperation getOperation() {
        return operation;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ObjectMap getKey() {
        return key;
    }

    public ObjectMap getBefore() {
        return before;
    }

    public ObjectMap getAfter() {
        return after;
    }

    public ObjectMap getValues() {
        return operation == DebeziumOperation.DELETE ? getBefore() : getAfter();
    }

    public String getUuid() {
        return getValues().getString("uuid");
    }

    public ObjectMap getSource() {
        return source;
    }

    @Override
    public String toString() {
        return getChangeEvent().toString();
    }
}