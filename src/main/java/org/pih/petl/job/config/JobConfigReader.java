package org.pih.petl.job.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.job.schedule.Schedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Utilities for reading in PetlJob Configuration Files, parsing these files, converting between formats, etc.
 */
@Component
public class JobConfigReader {

    @Autowired
    ApplicationConfig applicationConfig;

    /**
     * @return the configuration file at the path relative to the base configuration directory
     */
    public ConfigFile getConfigFile(String path) {
        return new ConfigFile(applicationConfig.getJobConfigDir(), path);
    }

    /**
     * @return the application config
     */
    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    public <T> T read(ConfigFile configFile, Class<T> type) {
        if (!configFile.exists()) {
            throw new PetlException("Configuration file not found: " + configFile);
        }
        try {
            JsonNode jsonNode = getYamlMapper().readTree(configFile.getConfigFile());
            if (type == JobConfig.class) {
                JobConfig jobConfig = new JobConfig();
                jobConfig.setType(jsonNode.get("type").asText());
                jobConfig.setConfiguration(jsonNode.get("configuration"));
                JsonNode scheduleNode = jsonNode.get("schedule");
                if (scheduleNode != null) {
                    jobConfig.setSchedule(getYamlMapper().treeToValue(scheduleNode, Schedule.class));
                }
                return (T)jobConfig;
            }
            else {
                return getYamlMapper().treeToValue(jsonNode, type);
            }
        }
        catch (Exception e) {
            throw new PetlException("Error parsing " + configFile + ", please check that the YML is valid", e);
        }
    }

    /**
     * @return a standard Yaml mapper that can be used for processing YML files
     */
    public static ObjectMapper getYamlMapper() {
        return new ObjectMapper(new YAMLFactory());
    }
}
