package org.pih.petl.job.streaming;

public enum DebeziumOperation {

    READ,
    INSERT,
    UPDATE,
    DELETE;

    public static DebeziumOperation parse(String operation) {
        if ("r".equals(operation)) {
            return READ;
        }
        else if ("i".equals(operation)) {
            return INSERT;
        }
        if ("u".equals(operation)) {
            return UPDATE;
        }
        if ("d".equals(operation)) {
            return DELETE;
        }
        throw new IllegalStateException("Unknown operation of '" + operation + "'. Unable to parse.");
    }
}