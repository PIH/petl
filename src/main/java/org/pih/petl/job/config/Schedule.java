package org.pih.petl.job.config;

/**
 * Encapsulates a basic schedule configuration for jobs
 */
public class Schedule {

    private String cron;

    public Schedule() {}

    @Override
    public String toString() {
        return cron;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
