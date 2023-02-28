package org.pih.petl.api;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Represents an ETL job execution and the status of this
 */
@Entity(name = "petl_job_state")
public class JobState {

    @EmbeddedId
    private Key key;

    @Column(name = "value", length = 1000)
    private String value;

    public JobState() {
    }

    public JobState(Key key, String value) {
        this.key = key;
        this.value = value;
    }

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Embeddable
    public static class Key implements Serializable {

        @Column(name = "job_key", length = 1000)
        private String jobKey;

        @Column(name ="property", length = 1000)
        private String property;

        public Key() {}

        public Key(String jobKey, String property) {
            this.jobKey = jobKey;
            this.property = property;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(jobKey).append(property).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) { return false; }
            if (!(obj instanceof JobState.Key)) { return false; }
            Key that = (Key) obj;
            return new EqualsBuilder().append(this.jobKey, that.jobKey).append(this.property, that.property).isEquals();
        }

        public String getJobKey() {
            return jobKey;
        }

        public void setJobKey(String jobKey) {
            this.jobKey = jobKey;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }
    }
}
