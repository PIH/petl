package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a particular ETL job configuration
 */
public class IteratingJob implements PetlJob {

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        context.setStatus("Executing IteratingJob");
        JobConfigReader configReader = new JobConfigReader(context);
        JobExecutor jobExecutor = new JobExecutor(configReader.getInt(1, "maxConcurrentJobs"));
        try {
            List<JsonNode> iterations = configReader.getList("iterations");
            List<JobExecutionTask> iterationTasks = new ArrayList<>();
            for (JsonNode iteration : iterations) {
                Map<String, String> iterationVars = configReader.getMap(iteration);
                for (String paramName : iterationVars.keySet()) {
                    String paramValue = iterationVars.get(paramName);
                    iterationVars.put(paramName, StrSubstitutor.replace(paramValue, context.getJobConfig().getParameters()));
                }
                JobConfig childConfig = configReader.getJobConfig(iterationVars, "jobTemplate");
                iterationTasks.add(new JobExecutionTask(new ExecutionContext(context, childConfig)));
                context.setStatus("Adding iteration task: " + iterationVars);
            }
            jobExecutor.executeInParallel(iterationTasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
