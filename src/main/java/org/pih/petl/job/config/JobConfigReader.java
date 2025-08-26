package org.pih.petl.job.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JobConfigReader {

    private ApplicationConfig appConfig;
    private JobConfig config;

    /**
     * @param appConfig the application config
     * @param config the job config
     */
    public JobConfigReader(ApplicationConfig appConfig, JobConfig config) {
        this.appConfig = appConfig;
        this.config = config;
    }

    /**
     * @param keys the nested keys to look up
     * @return the configuration setting at the nested level of configuration
     */
    public JsonNode get(String... keys) {
        JsonNode node = config == null ? null : config.getConfiguration();
        if (node == null || keys == null || keys.length == 0) {
            return node;
        }
        JsonNode ret = node.get(keys[0]);
        for (int i=1; i<keys.length; i++) {
            if (ret != null) {
                ret = ret.get(keys[i]);
            }
        }
        return ret;
    }

    /**
     * @param parameters the parameters
     * @param keys the keys
     * @return the JobConfig
     */
    public JobConfig getJobConfig(Map<String, String> parameters, String... keys) {
        JsonNode jobConfig = get(keys);
        if (jobConfig == null) {
            throw new PetlException("No job configuration found at path: " + arrayToString(keys));
        }
        try {
            Map<String, String> params = new LinkedHashMap<>(config.getParameters());
            params.putAll(parameters);
            return appConfig.getPetlJobConfig(config.getConfigFile(), params, jobConfig);
        }
        catch (Exception e) {
            throw new PetlException("Error reading job configuration at path: " + arrayToString(keys), e);
        }
    }

    /**
     * @param configNode the configNode
     * @return the JobConfig
     */
    public JobConfig getJobConfig(JsonNode configNode) {
        return appConfig.getPetlJobConfig(config.getConfigFile(), config.getParameters(), configNode);
    }

    /**
     * @param keys the keys
     * @return the file contents of the file indicated
     */
    public String getFileContents(String... keys) {
        String path = getString(keys);
        if (StringUtils.isNotEmpty(path)) {
            String fileContents = getFileContentsAtPath(getString(keys));
            if (StringUtils.isEmpty(fileContents)) {
                throw new PetlException("No file found at: " + arrayToString(keys) + " = " + path);
            }
            return fileContents;
        }
        return null;
    }

    /**
     * @param keys the keys
     * @return the file contents of the file indicated
     */
    public String getRequiredFileContents(String... keys) {
        String fileContents = getFileContents(keys);
        if (StringUtils.isEmpty(fileContents)) {
            throw new PetlException("Required configuration not found for: " + arrayToString(keys));
        }
        return fileContents;
    }

    /**
     * @param filePath the path
     * @return the file contents of the file indicated
     */
    public String getFileContentsAtPath(String filePath) {
        ConfigFile configFile = appConfig.getJobConfigFile(filePath);
        if (!configFile.getConfigFile().exists()) {
            return null;
        }
        try {
            return StrSubstitutor.replace(configFile.getContents(), config.getParameters());
        }
        catch (Exception e) {
            throw new PetlException("An error occurred trying to load file at " + filePath, e);
        }
    }

    /**
     * @param keys the keys
     * @return the DataSource
     */
    public DataSource getDataSource(String... keys) {
        String path = getString(keys);
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        return appConfig.getEtlDataSource(path);
    }

    /**
     * @param keys the keys
     * @return the List of Datasources
     */
    public List<DataSource> getDataSources(String... keys) {
        List<DataSource> ret = new ArrayList<>();
        for (String path : getStringList(keys)) {
            ret.add(appConfig.getEtlDataSource(path));
        }
        return ret;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     * @param keys the keys
     * @return the String at the keys
     */
    public String getString(String... keys) {
        return getString(get(keys));
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     * @param n the json node
     * @return the string at the node
     */
    public String getString(JsonNode n) {
        if (n != null) {
            return StrSubstitutor.replace(n.asText(), config.getParameters());
        }
        return null;
    }

    /**
     * @param defaultValue the default value
     * @param keys the keys
     * @return the Integer at the keys or default value if null
     */
    public Integer getInt(Integer defaultValue, String...keys) {
        JsonNode n = get(keys);
        if (n != null) {
            if (n.isInt()) {
                return n.asInt();
            }
            else {
                String stringVal = getString(n);
                if (StringUtils.isNotEmpty(stringVal)) {
                    return Integer.parseInt(stringVal);
                }
            }
        }
        return defaultValue;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     * @param defaultValue the default value
     * @param keys the keys
     * @return the Boolean at the keys or the default value if null
     */
    public Boolean getBoolean(Boolean defaultValue, String... keys) {
        JsonNode n = get(keys);
        if (n != null) {
            if (n.isBoolean()) {
                return n.asBoolean();
            }
            String stringVal = getString(n);
            if (StringUtils.isNotBlank(stringVal)) {
                return Boolean.parseBoolean(stringVal);
            }
            return false;
        }
        return defaultValue;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     * @param keys the keys
     * @return a List of JsonNode at the keys
     */
    public List<JsonNode> getList(String... keys) {
        List<JsonNode> ret = new ArrayList<>();
        JsonNode n = get(keys);
        if (n != null) {
            ArrayNode arrayNode = (ArrayNode)n;
            for (JsonNode arrayMember : arrayNode) {
                ret.add(arrayMember);
            }
        }
        return ret;
    }

    /**
     * @param n the node
     * @return the Map at the node
     */
    public Map<String, String> getMap(JsonNode n) {
        Map<String, String> m = new LinkedHashMap<>();
        if (n != null) {
            for (Iterator<String> i = n.fieldNames(); i.hasNext(); ) {
                String fieldName = i.next();
                String fieldValue = n.get(fieldName).asText();
                m.put(StrSubstitutor.replace(fieldName, config.getParameters()), StrSubstitutor.replace(fieldValue, config.getParameters()));
            }
        }
        return m;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     * @param keys the keys
     * @return the List of String at the keys
     */
    public List<String> getStringList(String... keys) {
        List<String> ret = new ArrayList<>();
        for (JsonNode n : getList(keys)) {
            ret.add(StrSubstitutor.replace(n.asText(), config.getParameters()));
        }
        return ret;
    }

    /**
     * Converts the YML specified within the configuration element into a properties format for the PetlJob
     * @return the config as Properties
     */
    public Properties getAsProperties() {
        Properties p = new Properties();
        p = addJsonNodeToProperties("", config.getConfiguration(), p);
        return p;
    }

    private String arrayToString(String... vals) {
        if (vals == null || vals.length == 0) {
            return "";
        }
        return Arrays.asList(vals).toString();
    }

    /**
     * Converts from a yml file to a properties file, using:
     *   - dot notation (eg. object1.nestedObject2.property)
     *   - array notation (eg. object1.nestedArray2[0].property)
     */
    private Properties addJsonNodeToProperties(String propertyName, JsonNode node, Properties p) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            for (Iterator<Map.Entry<String, JsonNode>> i = objectNode.fields(); i.hasNext();) {
                Map.Entry<String, JsonNode> entry = i.next();
                String newPropertyName = entry.getKey();
                if (propertyName != null && !propertyName.equals("")) {
                    newPropertyName = propertyName + "." + newPropertyName;
                }
                addJsonNodeToProperties(newPropertyName, entry.getValue(), p);
            }
        }
        else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                addJsonNodeToProperties(propertyName + "[" + i + "]", arrayNode.get(i), p);
            }
        }
        else if (node.isValueNode()) {
            ValueNode valueNode = (ValueNode) node;
            String value = valueNode.textValue();
            if (value != null) {
                p.put(propertyName, StrSubstitutor.replace(value, config.getParameters()));
            }
        }
        return p;
    }
}
