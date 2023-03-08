package org.pih.petl.api;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Represents an ETL job execution and the status of this
 */
@Entity(name = "petl_job_state")
public class JobState {

    @Id
    @Column(name = "key", length = 1000)
    private String key;

    @Column(name = "value", length = 1000)
    private String value;

    public JobState() {
    }

    public JobState(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
