package org.pih.petl;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PetlUtil {

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
            throw new RuntimeException("Unable to load properties from file at: " + filePath, e);
        }
        finally {
            IOUtils.closeQuietly(is);
        }
        return ret;
    }
}
