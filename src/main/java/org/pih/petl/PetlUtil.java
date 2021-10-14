package org.pih.petl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class PetlUtil {

    /**
     * @return a standard Yaml mapper that can be used for processing YML files
     */
    public static ObjectMapper getYamlMapper() {
        return new ObjectMapper(new YAMLFactory());
    }

    /**
     * @return a Properties object based on a properties file on the classpath at the specified location
     */
    public static Properties loadPropertiesFromFile(String filePath) {
        Properties ret = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
            ret.load(is);
        }
        catch (Exception e) {
            throw new PetlException("Unable to load properties from file at: " + filePath, e);
        }
        finally {
            IOUtils.closeQuietly(is);
        }
        return ret;
    }

    public static String getJsonAsString(JsonNode jsonNode) {
        try {
            return getYamlMapper().writeValueAsString(jsonNode);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to write Object as string");
        }
    }

    public static JsonNode readJsonFromString(String jsonString) {
        try {
            return getYamlMapper().readTree(jsonString);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to read Object from string");
        }
    }

    public static Map<String, String> getJsonAsMap(JsonNode jsonNode) {
        Map<String, String> m = new LinkedHashMap<>();
        for (Iterator<String> i = jsonNode.fieldNames(); i.hasNext();) {
            String fieldName = i.next();
            String fieldValue = jsonNode.get(fieldName).asText();
            m.put(fieldName, fieldValue);
        }
        return m;
    }
}
