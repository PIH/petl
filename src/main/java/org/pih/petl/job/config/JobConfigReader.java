package org.pih.petl.job.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.ExecutionContext;

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

    public JobConfigReader(ExecutionContext context) {
        this(context.getApplicationConfig(), context.getJobConfig());
    }
    
    public JobConfigReader(ApplicationConfig appConfig, JobConfig config) {
        this.appConfig = appConfig;
        this.config = config;
    }

    /**
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

    public JobConfig getJobConfig(JsonNode configNode) {
        return appConfig.getPetlJobConfig(config.getConfigFile(), config.getParameters(), configNode);
    }

    public String getFileContents(String... keys) {
        String path = getString(keys);
        if (StringUtils.isNotEmpty(path)) {
            return getFileContentsAtPath(getString(keys));
        }
        return null;
    }

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

    public DataSourceConfig getDataSource(String... keys) {
        String path = getString(keys);
        return appConfig.getEtlDataSource(path);
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public String getString(String... keys) {
        return getString(get(keys));
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public String getString(JsonNode n) {
        if (n != null) {
            return StrSubstitutor.replace(n.asText(), config.getParameters());
        }
        return null;
    }

    public Integer getInt(Integer defaultValue, String...keys) {
        JsonNode n = get(keys);
        if (n != null) {
            return n.asInt();
        }
        return defaultValue;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public Boolean getBoolean(Boolean defaultValue, String... keys) {
        JsonNode n = get(keys);
        if (n != null) {
            return n.asBoolean();
        }
        return defaultValue;
    }

    public <T> T getObject(Class<T> type, String... keys) {
        JsonNode n = get(keys);
        if (n != null) {
            try {
                return appConfig.getYamlMapper().treeToValue(n, type);
            }
            catch (Exception e) {
                throw new PetlException("Unable to read " + arrayToString(keys) + " as " + type.getSimpleName());
            }
        }
        return null;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public List<JsonNode> getList(String... keys) {
        List<JsonNode> ret = new ArrayList();
        JsonNode n = get(keys);
        if (n != null) {
            ArrayNode arrayNode = (ArrayNode)n;
            for (JsonNode arrayMember : arrayNode) {
                ret.add(arrayMember);
            }
        }
        return ret;
    }

    public <T> List<T> getList(Class<T> type, String... keys) {
        List<T> ret = new ArrayList<>();
        for (JsonNode n : getList(keys)) {
            try {
                ret.add(appConfig.getYamlMapper().treeToValue(n, type));
            }
            catch (Exception e) {
                throw new PetlException("Unable to read " + arrayToString(keys) + " as List of " + type.getSimpleName());
            }
        }
        return ret;
    }

    public Map<String, String> getMap(String... keys) {
        return getMap(get(keys));
    }

    public Map<String, String> getMap(JsonNode n) {
        Map<String, String> m = new LinkedHashMap<>();
        if (n != null) {
            for (Iterator<String> i = n.fieldNames(); i.hasNext(); ) {
                String fieldName = i.next();
                String fieldValue = n.get(fieldName).asText();
                m.put(fieldName, StrSubstitutor.replace(fieldValue, config.getParameters()));
            }
        }
        return m;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
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
