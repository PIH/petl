package org.pih.petl.job.config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.job.schedule.Schedule;

/**
 * Encapsulates a particular ETL job configuration
 */
public class JobConfig {

    private static final Log log = LogFactory.getLog(JobConfig.class);

    private String type;
    private Schedule schedule;
    private JsonNode configuration;

    public JobConfig() {}

    /**
     * @return the configuration setting at the nested level of configuration
     */
    public JsonNode get(String... keys) {
        if (configuration == null || keys == null || keys.length == 0) {
            return configuration;
        }
        JsonNode ret = configuration.get(keys[0]);
        for (int i=1; i<keys.length; i++) {
            if (ret != null) {
                ret = ret.get(keys[i]);
            }
        }
        return ret;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public String getString(String... keys) {
        JsonNode n = get(keys);
        if (n != null) {
            return n.asText();
        }
        return null;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public boolean getBoolean(String... keys) {
        JsonNode n = get(keys);
        if (n != null) {
            return n.asBoolean();
        }
        return false;
    }

    /**
     * Convenience to get the configuration of a given setting as a String
     */
    public List<String> getStringList(String... keys) {
        List ret = new ArrayList();
        JsonNode n = get(keys);
        if (n != null) {
            ArrayNode arrayNode = (ArrayNode)n;
            for (Iterator<JsonNode> i = arrayNode.iterator(); i.hasNext();) {
                JsonNode arrayMember = i.next();
                ret.add(arrayMember.asText());
            }
        }
        return ret;
    }

    /**
     * Converts the YML specified within the configuration element into a properties format for the PetlJob
     */
    public Properties getAsProperties() {
        Properties p = new Properties();
        p = addJsonNodeToProperties("", configuration, p);
        return p;
    }

    /**
     * Converts from a yml file to a properties file, using:
     *   - dot notation (eg. object1.nestedObject2.property)
     *   - array notation (eg. object1.nestedArray2[0].property)
     */
    private static Properties addJsonNodeToProperties(String propertyName, JsonNode node, Properties p) {
        log.debug("Adding json node to properties: " + propertyName);
        if (node.isObject()) {
            log.debug("Node is an object");
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
            log.debug("Node is an array");
            ArrayNode arrayNode = (ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                addJsonNodeToProperties(propertyName + "[" + i + "]", arrayNode.get(i), p);
            }
        }
        else if (node.isValueNode()) {
            log.debug("Node is a value.");
            ValueNode valueNode = (ValueNode) node;
            String value = valueNode.textValue();
            if (value != null) {
                log.debug("Adding value at: " + propertyName + " = " + value);
                p.put(propertyName, value);
            }
            else {
                log.warn("Value is null");
            }
        }
        return p;
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

    public JsonNode getConfiguration() {
        return configuration;
    }

    public void setConfiguration(JsonNode configuration) {
        this.configuration = configuration;
    }
}
