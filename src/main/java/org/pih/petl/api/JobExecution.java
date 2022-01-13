package org.pih.petl.api;

import org.apache.commons.lang.exception.ExceptionUtils;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import java.util.Date;
import java.util.UUID;

/**
 * Represents an ETL job execution and the status of this
 */
@Entity(name = "petl_job_execution")
public class JobExecution {

    @Id
    private String uuid;

    @Column(name = "job_path", length = 1000)
    private String jobPath;

    @Column(name ="parent_execution_uuid", length = 36)
    private String parentExecutionUuid;

    @Column(name = "description", length = 1000)
    private String description;

    @Column(name = "initiated", nullable = false)
    private Date initiated;

    @Column(name = "started")
    private Date started;

    @Column(name = "completed")
    private Date completed;

    @Column(name = "status", nullable = false, length = 50)
    @Enumerated(EnumType.STRING)
    private JobExecutionStatus status;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    public JobExecution() {}

    public JobExecution(String jobPath) {
        this(jobPath, null, null);
    }

    public JobExecution(String jobPath, String parentExecutionUuid, String description) {
        this.uuid = UUID.randomUUID().toString();
        this.jobPath = jobPath;
        this.parentExecutionUuid = parentExecutionUuid;
        this.description = description;
        this.initiated = new Date();
        this.status = JobExecutionStatus.INITIATED;
    }

    public int getDurationSeconds() {
        if (started == null) { return 0; }
        long st = started.getTime();
        long ed = (completed == null ? new Date() : completed).getTime();
        return (int)(ed-st)/1000;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Job " + "(" + uuid + "): " + status);
        if (jobPath != null) {
            sb.append(", path: " + jobPath);
        }
        if (description != null) {
            sb.append(", description: " + description);
        }
        if (initiated != null) {
            sb.append(", initiated: " + initiated);
        }
        if (started != null) {
            sb.append(", started: " + started);
            if (completed != null) {
                sb.append(", completed: " + completed);
                sb.append(", duration: " + getDurationSeconds());
            }
        }
        if (errorMessage != null) {
            sb.append(" ERROR: " + errorMessage);
        }
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getJobPath() {
        return jobPath;
    }

    public void setJobPath(String jobPath) {
        this.jobPath = jobPath;
    }

    public String getParentExecutionUuid() {
        return parentExecutionUuid;
    }

    public void setParentExecutionUuid(String parentExecutionUuid) {
        this.parentExecutionUuid = parentExecutionUuid;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public JobExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(JobExecutionStatus status) {
        this.status = status;
    }

    public Date getInitiated() {
        return initiated;
    }

    public void setInitiated(Date initiated) {
        this.initiated = initiated;
    }

    public Date getStarted() {
        return started;
    }

    public void setStarted(Date started) {
        this.started = started;
    }

    public Date getCompleted() {
        return completed;
    }

    public void setCompleted(Date completed) {
        this.completed = completed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setErrorMessageFromException(Throwable t) {
        String message = ExceptionUtils.getMessage(t);
        if (message.length() > 1000) {
            message = message.substring(0, 1000);
        }
        setErrorMessage(message);
    }
}
