package org.pih.petl.api.status;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Represents an ETL job execution and the status of this
 */
public class EtlStatus {

    private static Log log = LogFactory.getLog(EtlStatus.class);

    private String uuid;
    private Integer num;
    private String tableName;
    private Integer totalExpected;
    private Integer totalLoaded;
    private Date started;
    private Date completed;
    private String status;
    private String errorMessage;

    public EtlStatus() {}

    public EtlStatus(String uuid, String tableName) {
        this.uuid = uuid;
        this.tableName = tableName;
        this.started = new Date();
    }

    public int getDurationSeconds() {
        if (started == null) { return 0; }
        long st = started.getTime();
        long ed = (completed == null ? new Date() : completed).getTime();
        return (int)(ed-st)/1000;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
