package org.pih.petl.api;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.job.config.JobConfig;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Lob;
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

    @Column(name ="sequence_num")
    private Integer sequenceNum;

    @Column(name = "description", length = 1000)
    private String description;

    @Lob
    @Column(name = "config")
    private String config;

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

    private transient JobConfig jobConfig;

    public JobExecution() {
        this.uuid = UUID.randomUUID().toString();
        this.initiated = new Date();
        this.status = JobExecutionStatus.INITIATED;
    }

    public JobExecution(JobConfig jobConfig) {
        this();
        this.description = jobConfig.getDescription();
        this.jobConfig = jobConfig;
        try {
            this.config = ApplicationConfig.getYamlMapper().writeValueAsString(jobConfig);
        }
        catch (Exception e) {
            throw new PetlException("Unable to write job configuration to json", e);
        }
    }

    public JobExecution(String jobPath, JobConfig jobConfig) {
        this(jobConfig);
        this.jobPath = jobPath;
    }

    public JobExecution(JobExecution parentExecution, JobConfig jobConfig, Integer sequenceNum) {
        this(jobConfig);
        this.description = jobConfig.getDescription();
        this.parentExecutionUuid = parentExecution.getUuid();
        this.sequenceNum = sequenceNum;
    }

    public int getDurationSeconds() {
        if (started == null) { return 0; }
        long st = started.getTime();
        long ed = (completed == null ? new Date() : completed).getTime();
        return (int)(ed-st)/1000;
    }

    public JobConfig getJobConfig() {
        if (jobConfig == null) {
            try {
                jobConfig = ApplicationConfig.getYamlMapper().readValue(config, JobConfig.class);
            }
            catch (Exception e) {
                throw new PetlException("Unable to read job configuration from json", e);
            }
        }
        return jobConfig;
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

    public Integer getSequenceNum() {
        return sequenceNum;
    }

    public void setSequenceNum(Integer sequenceNum) {
        this.sequenceNum = sequenceNum;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
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
