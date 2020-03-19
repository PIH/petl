package org.pih.petl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.datasource.EtlDataSource;
import org.pih.petl.job.schedule.Schedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Utilities for reading in PetlJob Configuration Files, parsing these files, converting between formats, etc.
 */
@Component
public class ApplicationConfig {

    private static final Log log = LogFactory.getLog(ApplicationConfig.class);

    public static final String PETL_HOME_DIR = "petl.homeDir";
    public static final String PETL_JOB_DIR = "petl.jobDir";
    public static final String PETL_DATASOURCE_DIR = "petl.datasourceDir";

    private Map<String, String> env = null;

    @Autowired
    Environment environment;

    /**
     * @return a Map of the environment that PETL is running in (Environment variables and system properties, etc)
     */
    public Map<String, String> getEnv() {
        if (env == null) {
            env = new HashMap<>();
            Set<String> propertyNames = new TreeSet<String>();
            for (PropertySource s : ((AbstractEnvironment)environment).getPropertySources()) {
                if (s instanceof EnumerablePropertySource) {
                    for (String propertyName : ((EnumerablePropertySource)s).getPropertyNames()) {
                        propertyNames.add(propertyName);
                    }
                }
            }
            for (String propertyName : propertyNames) {
                env.put(propertyName, environment.getProperty(propertyName));
            }
        }
        return env;
    }

    /**
     * @return the File representing the PETL_HOME directory
     */
    public File getDirectoryFromEnvironment(String envName, boolean required) {
        log.debug("Loading " + envName + " from environment");
        String path = environment.getProperty(envName);
        if (StringUtils.isBlank(path)) {
            if (required) {
                throw new PetlException("The " + envName + " environment variable is required.");
            }
            else {
                return null;
            }
        }
        log.debug(envName + " configuration found: " + path);
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) {
            String message = envName + " = " + path + " does not point to a valid directory";
            if (required) {
                throw new PetlException(message);
            }
            else {
                log.warn(message);
                return null;
            }
        }
        return dir;
    }

    /**
     * @return the File representing the PETL_HOME directory
     */
    public File getPetlHomeDir() {
        return getDirectoryFromEnvironment(PETL_HOME_DIR, true);
    }

    /**
     * @return the directory in which job configurations are found for this PETL instance
     */
    public File getJobDir() {
        return getDirectoryFromEnvironment(PETL_JOB_DIR, true);
    }

    /**
     * @return the directory in which data source configurations are found for this PETL instance
     */
    public File getDataSourceDir() {
        return getDirectoryFromEnvironment(PETL_DATASOURCE_DIR, true);
    }

    /**
     * Convenience method to retrieve a PETL Job Config with the given path
     */
    public PetlJobConfig getPetlJobConfig(String path) {
        ConfigFile configFile = new ConfigFile(getJobDir(), path);
        return loadConfiguration(configFile, PetlJobConfig.class);
    }

    /**
     * Convenience method to retrieve a PETL Job Config with the given path
     */
    public ConfigFile getConfigFile(String path) {
        return new ConfigFile(getJobDir(), path);
    }

    /**
     * Convenience method to retrieve an EtlDataSource with the given path
     */
    public EtlDataSource getEtlDataSource(String path) {
        ConfigFile configFile = new ConfigFile(getDataSourceDir(), path);
        return loadConfiguration(configFile, EtlDataSource.class);
    }

    /**
     * @return the configuration file specified, loaded into the given type
     */
    public <T> T loadConfiguration(ConfigFile configFile, Class<T> type) {
        if (!configFile.exists()) {
            throw new PetlException("Configuration file not found: " + configFile);
        }
        try {
            String fileContents = FileUtils.readFileToString(configFile.getConfigFile(), "UTF-8");
            String fileWithVariablesReplaced = StrSubstitutor.replace(fileContents, getEnv());
            JsonNode jsonNode = getYamlMapper().readTree(fileWithVariablesReplaced);
            if (type == PetlJobConfig.class) {
                PetlJobConfig jobConfig = new PetlJobConfig();
                jobConfig.setConfigFile(configFile);
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
