package org.pih.petl.job.config;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.pih.petl.PetlException;

/**
 * Represents a configuration file
 */
public class ConfigFile {

    private File fileDir;
    private String filePath;
    private File configFile;

    public ConfigFile(File fileDir, String filePath) {
        this.fileDir = fileDir;
        this.filePath = filePath;
        this.configFile = new File(fileDir, filePath);
    }

    @Override
    public String toString() {
        return configFile.getAbsolutePath();
    }

    public boolean exists() {
        return configFile.exists();
    }

    public String getContents() {
        if (configFile == null) {
            throw new PetlException("Unable to find configuration file at " + configFile);
        }
        try {
            return FileUtils.readFileToString(configFile, "UTF-8");
        }
        catch (Exception e) {
            throw new PetlException("Error reading " + configFile + ", please check that the file is valid", e);
        }
    }

    public File getFileDir() {
        return fileDir;
    }

    public void setFileDir(File fileDir) {
        this.fileDir = fileDir;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public File getConfigFile() {
        return configFile;
    }

    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }
}
