package org.pih.petl.job.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encapsulates a particular ETL job configuration
 */
public class JobConfig {

    @JsonIgnore
    private ConfigFile configFile; // This is the file in which this job is configured.  This may be nested in another config.

    private String path;

    private String type;

    private JsonNode configuration;

    private Map<String, String> parameters;

    private Schedule schedule;

    public JobConfig() {}

    @Override
    public String toString() {
        Map<String, String> ret = new LinkedHashMap<>();
        if (path != null) {
            ret.put("path", path);
        }
        if (type != null) {
            ret.put("type", type);
        }
        if (configuration != null) {
            ret.put("configuration", configuration.toString());
        }
        return ret.toString();
    }

    public ConfigFile getConfigFile() {
        return configFile;
    }

    public void setConfigFile(ConfigFile configFile) {
        this.configFile = configFile;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public JsonNode getConfiguration() {
        return configuration;
    }

    public void setConfiguration(JsonNode configuration) {
        this.configuration = configuration;
    }

    public Map<String, String> getParameters() {
        if (parameters == null) {
            parameters = new LinkedHashMap<>();
        }
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }
}
