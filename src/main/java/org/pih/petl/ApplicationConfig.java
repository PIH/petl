package org.pih.petl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.config.ConfigFile;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.DataSourceConfig;
import org.pih.petl.job.config.Schedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utilities for reading in PetlJob Configuration Files, parsing these files, converting between formats, etc.
 */
@Component
public class ApplicationConfig {

    private static final Log log = LogFactory.getLog(ApplicationConfig.class);

    public static final String PETL_HOME_DIR = "petl.homeDir";
    public static final String PETL_JOB_DIR = "petl.jobDir";
    public static final String PETL_DATASOURCE_DIR = "petl.datasourceDir";
    public static final String PETL_SCHEDULE_CRON = "petl.schedule.cron";
    public static final String PETL_STARTUP_JOBS = "petl.startup.jobs";

    public static final ObjectMapper getYamlMapper() {
        return new ObjectMapper(new YAMLFactory());
    }

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
        log.trace("Loading " + envName + " from environment");
        String path = environment.getProperty(envName);
        if (StringUtils.isBlank(path)) {
            if (required) {
                throw new PetlException("The " + envName + " environment variable is required.");
            }
            else {
                return null;
            }
        }
        log.trace(envName + " configuration found: " + path);
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
     * @return get any schedule set globally for this PETL instance
     */
    public Schedule getSchedule() {
        if (environment.getProperty(PETL_SCHEDULE_CRON) != null) {
            Schedule schedule = new Schedule();
            schedule.setCron(environment.getProperty(PETL_SCHEDULE_CRON));
            return schedule;
        }
        else {
            return null;
        }
    }

    public List<String> getStartupJobs() {
        List<String> l = new ArrayList<>();
        boolean allFound = false;
        int index = 0;
        while (!allFound) {
            String val = environment.getProperty(PETL_STARTUP_JOBS + "[" + index++ + "]");
            if (val != null) {
                l.add(val);
            }
            else {
                allFound = true;
            }
        }
        return l;
    }

    public ConfigFile getJobConfigFile(String path) {
        return new ConfigFile(getJobDir(), path);
    }

    /**
     * Convenience method to retrieve a PETL Job Config with the given path
     */
    public JobConfig getPetlJobConfig(String path) {
        ConfigFile configFile = getJobConfigFile(path);
        if (!configFile.exists()) {
            throw new PetlException("Job Configuration file not found: " + configFile);
        }
        return getPetlJobConfig(configFile, getEnv());
    }

    public JobConfig getPetlJobConfig(ConfigFile configFile, Map<String, String> parameters) {
        try {
            String fileContents = FileUtils.readFileToString(configFile.getConfigFile(), "UTF-8");
            JsonNode configNode = readJsonFromString(fileContents);
            return getPetlJobConfig(configFile, parameters, configNode);
        }
        catch (Exception e) {
            throw new PetlException("Error parsing " + configFile + ", please check that the YML is valid", e);
        }
    }

    public JobConfig getPetlJobConfig(ConfigFile configFile, Map<String, String> parameters, JsonNode configNode) {
        try {
            JobConfig config = getYamlMapper().treeToValue(configNode, JobConfig.class);
            config.setConfigFile(configFile);
            Map<String, String> newParameters = new LinkedHashMap<>(parameters);
            for (String parameter : config.getParameters().keySet()) {
                String value = config.getParameters().get(parameter);
                newParameters.put(parameter, StrSubstitutor.replace(value, parameters));
            }
            config.setParameters(newParameters);
            if (StringUtils.isNotEmpty(config.getPath())) {
                ConfigFile configFileAtPath = getJobConfigFile(config.getPath());
                if (!configFileAtPath.exists()) {
                    throw new PetlException("Job Configuration file not found: " + config.getPath());
                }
                JobConfig configAtPath = getPetlJobConfig(configFileAtPath, config.getParameters());
                configAtPath.setConfigFile(config.getConfigFile());
                config = configAtPath;
            }
            return config;
        }
        catch (Exception e) {
            throw new PetlException("Error reading json configuration as a PetlJobConfig", e);
        }
    }

    /**
     * Convenience method to retrieve an EtlDataSource with the given path
     */
    public DataSourceConfig getEtlDataSource(String path) {
        ConfigFile configFile = new ConfigFile(getDataSourceDir(), path);
        if (!configFile.exists()) {
            throw new PetlException("ETL Datasource file not found: " + configFile);
        }
        try {
            String fileContents = FileUtils.readFileToString(configFile.getConfigFile(), "UTF-8");
            String fileWithVariablesReplaced = StrSubstitutor.replace(fileContents, getEnv());
            return getYamlMapper().readValue(fileWithVariablesReplaced, DataSourceConfig.class);
        }
        catch (Exception e) {
            throw new PetlException("Error reading " + path + " as an ETLDataSource", e);
        }
    }

    public String getJsonAsString(JsonNode jsonNode) {
        if (jsonNode != null) {
            try {
                return getYamlMapper().writeValueAsString(jsonNode);
            }
            catch (Exception e) {
                throw new IllegalStateException("Unable to write Object as string");
            }
        }
        return null;
    }

    public JsonNode readJsonFromString(String jsonString) {
        try {
            return getYamlMapper().readTree(jsonString);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to read Object from string");
        }
    }
}
