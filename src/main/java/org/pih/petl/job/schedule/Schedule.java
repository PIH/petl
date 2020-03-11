package org.pih.petl.job.schedule;

/**
 * Encapsulates a basic schedule configuration for jobs
 */
public class Schedule {

    private String cron;

    public Schedule() {}

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
