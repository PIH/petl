package org.pih.petl;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.schedule.Schedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Utilities for reading in PetlJob Configuration Files, parsing these files, converting between formats, etc.
 */
@Component
public class ApplicationConfig {

    private static final Log log = LogFactory.getLog(ApplicationConfig.class);

    public static final String ENV_PETL_HOME = "PETL_HOME";
    public static final String JOB_CONFIG_DIR = "petl.jobDir";

    @Autowired
    Environment environment;

    /**
     * @return the directory in which job configurations are found for this PETL instance
     */
    public File getJobConfigDir() {
        File ret = new File(getPetlHomeDir(), "config");
        String jobDir = environment.getProperty(JOB_CONFIG_DIR);
        if (StringUtils.isBlank(jobDir)) {
            log.warn("No value specified for " + JOB_CONFIG_DIR + ", using default of " + ret.getAbsolutePath());
        }
        else {
            ret = new File(jobDir);
        }
        if (!ret.exists()) {
            throw new PetlException("The configuration directory does not exist: " + ret.getAbsolutePath());
        }
        return ret;
    }

    /**
     * @return the configuration file at the path relative to the base configuration directory
     */
    public ConfigFile getConfigFile(String path) {
        return new ConfigFile(getJobConfigDir(), path);
    }

    /**
     * @return the File representing the PETL_HOME directory
     */
    public File getPetlHomeDir() {
        log.debug("Loading home directory configuration from environment variable");
        String path = System.getenv(ENV_PETL_HOME);
        if (StringUtils.isBlank(path)) {
            log.debug("Not found, loading home dir from system property");
            path = System.getProperty(ENV_PETL_HOME);
            if (StringUtils.isBlank(path)) {
                throw new PetlException("The " + ENV_PETL_HOME + " environment variable is required.");
            }
        }
        log.debug("Home directory configuration found: " + path);
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new PetlException("The " + ENV_PETL_HOME + " setting of <" + path + ">" + " does not point to a valid directory");
        }
        return dir;
    }

    /**
     * @return the File representing the log file
     */
    public File getLogFile() {
        File dir = new File(getPetlHomeDir(), "logs");
        if (!dir.exists()) {
            dir.mkdir();
        }
        return new File(dir, "petl.log");
    }

    /**
     * @return the configuration file specified, loaded into the given type
     */
    public <T> T loadConfiguration(ConfigFile configFile, Class<T> type) {
        if (!configFile.exists()) {
            throw new PetlException("Configuration file not found: " + configFile);
        }
        try {
            JsonNode jsonNode = getYamlMapper().readTree(configFile.getConfigFile());
            if (type == PetlJobConfig.class) {
                PetlJobConfig jobConfig = new PetlJobConfig();
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
