package org.pih.petl.api;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Date;

/**
 * Represents an ETL job execution and the status of this
 */
@Entity(name = "petl_job_execution")
public class JobExecution {

    @Id
    private String uuid;

    @Column(name = "job_path", nullable = false, length = 100)
    private String jobPath;

    @Column(name = "started", nullable = false)
    private Date started;

    @Column(name = "completed")
    private Date completed;

    @Column(name = "status", nullable = false, length = 50)
    private JobExecutionStatus status;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    public JobExecution() {}

    public JobExecution(String uuid, String jobPath) {
        this.uuid = uuid;
        this.jobPath = jobPath;
        this.started = new Date();
        this.status = JobExecutionStatus.IN_PROGRESS;
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
        sb.append("Job " + jobPath + " (" + uuid + "): " + status);
        if (started != null) {
            sb.append(", started: " + started);
            if (completed != null) {
                sb.append(", completed: " + completed);
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

    public JobExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(JobExecutionStatus status) {
        this.status = status;
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
}
