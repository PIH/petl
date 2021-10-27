package org.pih.petl.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlUtil;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.config.PetlJobConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the status of a given execution
 */
public class ExecutionContext {

    private static Log log = LogFactory.getLog(ExecutionContext.class);

    private JobExecution jobExecution;
    private PetlJobConfig jobConfig;
    private ApplicationConfig applicationConfig;
    private String status;
    private int totalExpected;
    private int totalLoaded;

    public ExecutionContext(JobExecution jobExecution, PetlJobConfig jobConfig, ApplicationConfig applicationConfig) {
        this.jobExecution = jobExecution;
        this.jobConfig = jobConfig;
        this.applicationConfig = applicationConfig;
    }

    public PetlJobConfig getNestedJobConfig(JsonNode jobTemplate, Map<String, String> replacementVariables) {
        JsonNode pathNode = jobTemplate.get("path");
        if (pathNode != null) {
            String path = pathNode.asText();
            return applicationConfig.getPetlJobConfig(path);
        }
        else {
            PetlJobConfig nestedJobConfig = new PetlJobConfig();
            nestedJobConfig.setConfigFile(jobConfig.getConfigFile());
            nestedJobConfig.setType(jobTemplate.get("type").asText());
            String configuration = PetlUtil.getJsonAsString(jobTemplate.get("configuration"));
            Map<String, String> replacements = new HashMap<>(jobConfig.getConfiguration().getVariables());
            if (replacementVariables != null) {
                replacements.putAll(replacementVariables);
            }
            configuration = StrSubstitutor.replace(configuration, replacements);
            JobConfiguration jobConfiguration = new JobConfiguration(PetlUtil.readJsonFromString(configuration));
            jobConfiguration.setVariables(replacements);
            nestedJobConfig.setConfiguration(jobConfiguration);
            return nestedJobConfig;
        }
    }

    public JobExecution getJobExecution() {
        return jobExecution;
    }

    public PetlJobConfig getJobConfig() {
        return jobConfig;
    }

    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        log.debug(jobConfig.getType() + ": " + status);
        this.status = status;
    }

    public int getTotalExpected() {
        return totalExpected;
    }

    public void setTotalExpected(int totalExpected) {
        this.totalExpected = totalExpected;
    }

    public int getTotalLoaded() {
        return totalLoaded;
    }

    public void setTotalLoaded(int totalLoaded) {
        this.totalLoaded = totalLoaded;
        log.trace(jobConfig.getType() + ": " + status + " loaded " + totalLoaded + "/" + totalExpected);
    }
}
