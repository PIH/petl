package org.pih.petl;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        File ret = new File(getHomeDir(), "config");
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
     * @return the path to where PETL is installed, defined by the PETL_HOME environment variable
     */
    public String getHomePath() {
        String path = System.getenv(ENV_PETL_HOME);
        if (StringUtils.isBlank(path)) {
            path = System.getProperty(ENV_PETL_HOME);
            if (StringUtils.isBlank(path)) {
                throw new PetlException("The " + ENV_PETL_HOME + " environment variable is required.");
            }
        }
        return path;
    }

    /**
     * @return the File representing the PETL_HOME directory
     */
    public File getHomeDir() {
        String path = getHomePath();
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
        File dir = new File(getHomeDir(), "logs");
        if (!dir.exists()) {
            dir.mkdir();
        }
        return new File(dir, "petl.log");
    }
}
