package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.ExecutionConfig;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a particular ETL job configuration
 */
public class IteratingJob implements PetlJob {

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
        context.setStatus("Executing IteratingJob");
        JobConfigReader configReader = new JobConfigReader(context);

        ExecutionConfig executionConfig = configReader.getObject(ExecutionConfig.class, "execution");
        if (executionConfig == null) {
            executionConfig = new ExecutionConfig(1, 0, 5, TimeUnit.MINUTES);
        }

        List<JsonNode> iterations = configReader.getList("iterations");
        List<JobExecutionTask> iterationTasks = new ArrayList<>();
        for (JsonNode iteration : iterations) {
            try {
                Map<String, String> iterationVars = configReader.getMap(iteration);
                for (String paramName : iterationVars.keySet()) {
                    String paramValue = iterationVars.get(paramName);
                    iterationVars.put(paramName, StrSubstitutor.replace(paramValue, context.getJobConfig().getParameters()));
                }
                JobConfig config = configReader.getJobConfig(iterationVars, "jobTemplate");
                context.setStatus("Adding iteration task: " + iterationVars);
                iterationTasks.add(new JobExecutionTask(config, context, executionConfig));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        JobExecutor.execute(iterationTasks, context, executionConfig);
    }
}
