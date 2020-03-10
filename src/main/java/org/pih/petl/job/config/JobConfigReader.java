package org.pih.petl.job.config;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
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
    public File getConfigFile(String path) {
        return new File(applicationConfig.getJobConfigDir(), path);
    }

    /**
     * @return the application config
     */
    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    /**
     * @return file contents as String represented by the file
     */
    public String getFileContents(File f) {
        if (f == null) {
            throw new PetlException("Unable to find configuration file at " + f);
        }
        try {
            return FileUtils.readFileToString(f, "UTF-8");
        }
        catch (Exception e) {
            throw new PetlException("Error parsing " + f + ", please check that the file is valid", e);
        }
    }

    public <T> T getConfigurationFromFile(File f, Class<T> type) {
        if (f == null) {
            throw new PetlException("Unable to find " + type.getSimpleName() + " file at " + f);
        }
        try {
            JsonNode jsonNode = getYamlMapper().readTree(f);
            return getYamlMapper().treeToValue(jsonNode, type);
        }
        catch (Exception e) {
            throw new PetlException("Error parsing " + f + ", please check that the YML is valid", e);
        }
    }

    /**
     * @return an EtlDataSource represented by the file at the given path
     */
    public JobConfig getEtlJobConfigFromFile(File f) {
        if (f == null) {
            throw new PetlException("Unable to find job config file at " + f);
        }
        try {
            JsonNode jsonNode = getYamlMapper().readTree(f);
            return new JobConfig(jsonNode);
        }
        catch (Exception e) {
            throw new PetlException("Error parsing " + f + ", please check that the YML is valid", e);
        }
    }

    /**
     * @return a standard Yaml mapper that can be used for processing YML files
     */
    public static ObjectMapper getYamlMapper() {
        return new ObjectMapper(new YAMLFactory());
    }
}
