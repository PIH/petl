package org.pih.petl.job.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.pih.petl.PetlUtil;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ExecutionConfig;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.config.PetlJobConfig;

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
        context.setStatus("Executing Iterating Job");
        JobConfiguration config = context.getJobConfig().getConfiguration();

        JsonNode jobTemplate = config.get("jobTemplate");
        List<JsonNode> iterations = config.getList("iterations");
        ExecutionConfig executionConfig = config.getObject(ExecutionConfig.class, "execution");
        if (executionConfig == null) {
            executionConfig = new ExecutionConfig(1, 0, 5, TimeUnit.MINUTES);
        }
        context.setTotalExpected(iterations.size());
        List<JobExecutionTask> iterationTasks = new ArrayList<>();
        for (JsonNode iteration : iterations) {
            Map<String, String> iterationVars = PetlUtil.getJsonAsMap(iteration);
            context.setStatus("Adding iteration task: " + iterationVars);
            PetlJobConfig jobConfig = context.getNestedJobConfig(jobTemplate, iterationVars);
            iterationTasks.add(new JobExecutionTask(jobConfig, context, executionConfig));
        }

        JobExecutor.execute(iterationTasks, context, executionConfig);
    }
}
