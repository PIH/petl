package org.pih.petl.job.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.schedule.Schedule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Encapsulates a particular ETL job configuration
 */
public class PetlJobConfig {

    private static final Log log = LogFactory.getLog(PetlJobConfig.class);

    private ConfigFile configFile;
    private String type;
    private Schedule schedule;
    private JobConfiguration configuration;

    public PetlJobConfig() {}

    @Override
    public String toString() {
        return configFile + ": " + configuration;
    }

    public ConfigFile getConfigFile() {
        return configFile;
    }

    public void setConfigFile(ConfigFile configFile) {
        this.configFile = configFile;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public JobConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(JobConfiguration configuration) {
        this.configuration = configuration;
    }
}
