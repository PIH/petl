package org.pih.petl.job.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlUtil;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionResult;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.config.PetlJobConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        context.setStatus("Executing Iterating Job");
        JobConfiguration config = context.getJobConfig().getConfiguration();

        List<JsonNode> jobTemplates = config.getList("jobTemplates");
        List<JsonNode> iterations = config.getList("iterations");

        int maxConcurrentIterations = config.getInt(1, "execution", "maxConcurrentIterations");
        int maxRetries = config.getInt(0, "execution", "maxRetries");
        int retryIntervalSeconds = config.getInt(300, "execution", "retryIntervalSeconds");

        ExecutorService executorService = Executors.newFixedThreadPool(maxConcurrentIterations);

        List<JobExecutionTask> iterationTasks = new ArrayList<>();
        for (JsonNode iteration : iterations) {
            Map<String, String> iterationVars = PetlUtil.getJsonAsMap(iteration);
            context.setStatus("Adding iteration task: " + iterationVars);
            List<PetlJobConfig> jobConfigs = new ArrayList<>();
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

                jobConfigs.add(petlJobConfig);
            }
            iterationTasks.add(new JobExecutionTask(jobConfigs, context, maxRetries, retryIntervalSeconds));
        }

        context.setStatus("Executing iteration tasks");
        List<Future<JobExecutionResult>> results = executorService.invokeAll(iterationTasks);
        executorService.shutdown();

        // We only arrive here once all Tasks have been completed

        for (Future<JobExecutionResult> resultFutures : results) {
            JobExecutionResult result = resultFutures.get();
            if (!result.isSuccessful()) {
                throw new RuntimeException("Error in one of the jobs", result.getException());
            }
        }
    }
}
