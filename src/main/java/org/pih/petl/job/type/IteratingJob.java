package org.pih.petl.job.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlUtil;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a particular ETL job configuration
 */
public class IteratingJob implements PetlJob {

    private static Log log = LogFactory.getLog(IteratingJob.class);

    /**
     * Creates a new instance of the job
     */
    public IteratingJob() {
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        ApplicationConfig appConfig = context.getApplicationConfig();
        JobConfiguration config = context.getJobConfig().getConfiguration();

        List<JsonNode> jobTemplates = config.getList("jobTemplates");
        List<JsonNode> iterations = config.getList("iterations");

        // TODO:  Add in threading (run each iteration in a separate thread
        // TODO:  Add retries on connection failure errorHanding.retryInterval, maxRetries, etc.
        for (JsonNode iteration : iterations) {
            Map<String, String> iterationVars = PetlUtil.getJsonAsMap(iteration);
            context.setStatus("Executing iteration: " + iterationVars);
            for (JsonNode jobTemplate : jobTemplates) {

                PetlJobConfig petlJobConfig = new PetlJobConfig();
                petlJobConfig.setType(jobTemplate.get("type").asText());
                String configuration = PetlUtil.getJsonAsString(jobTemplate.get("configuration"));

                Map<String, String> replacements = new HashMap<>(config.getVariables());
                replacements.putAll(iterationVars);

                configuration = StrSubstitutor.replace(configuration, replacements);
                JobConfiguration jobConfiguration = new JobConfiguration(PetlUtil.readJsonFromString(configuration));
                jobConfiguration.setVariables(replacements);
                petlJobConfig.setConfiguration(jobConfiguration);

                context.setStatus("Executing job: " + petlJobConfig.getType());
                ExecutionContext nestedContext = new ExecutionContext(context.getJobExecution(), petlJobConfig, appConfig);
                PetlJob job = PetlJobFactory.instantiate(petlJobConfig);
                job.execute(nestedContext);
            }
        }
    }
}
