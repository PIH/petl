package org.pih.petl.api;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Represents an ETL job execution and the status of this
 */
@Entity(name = "petl_job_execution")
public class JobExecution {

    private static Log log = LogFactory.getLog(JobExecution.class);

    @Id
    private String uuid;

    @Column(name = "job_path", nullable = false, length = 100)
    private String jobPath;

    @Column(name = "total_expected")
    private Integer totalExpected;

    @Column(name = "total_loaded")
    private Integer totalLoaded;

    @Column(name = "started", nullable = false)
    private Date started;

    @Column(name = "completed")
    private Date completed;

    @Column(name = "status", nullable = false, length = 1000)
    private String status;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    public JobExecution() {}

    public JobExecution(String uuid, String jobPath) {
        this.uuid = uuid;
        this.jobPath = jobPath;
        this.started = new Date();
        this.status = "Execution Initiated";
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
        if (totalLoaded != null && totalExpected != null) {
            sb.append(" ").append(totalLoaded + "/" + totalExpected);
        }
        if (started != null && completed != null) {
            sb.append(" in " + getDurationSeconds() + " seconds");
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

    public Integer getTotalExpected() {
        return totalExpected;
    }

    public void setTotalExpected(Integer totalExpected) {
        this.totalExpected = totalExpected;
    }

    public Integer getTotalLoaded() {
        return totalLoaded;
    }

    public void setTotalLoaded(Integer totalLoaded) {
        this.totalLoaded = totalLoaded;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
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
