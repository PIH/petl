package org.pih.petl.api;

public enum JobExecutionStatus {
    INITIATED,
    QUEUED,
    IN_PROGRESS,
    FAILED_WILL_RETRY,
    RETRY_QUEUED,
    SUCCEEDED,
    FAILED,
    ABORTED
}
