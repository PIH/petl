package org.pih.petl.job.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.ExecutionConfig;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.config.PetlJobConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a particular ETL job configuration
 */
public class RunMultipleJob implements PetlJob {

    private static Log log = LogFactory.getLog(RunMultipleJob.class);

    /**
     * Creates a new instance of the job
     */
    public RunMultipleJob() {
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        JobConfiguration config = context.getJobConfig().getConfiguration();

        List<JsonNode> jobTemplates = config.getList("jobs");
        ExecutionConfig executionConfig = config.getObject(ExecutionConfig.class, "execution");
        if (executionConfig == null) {
            executionConfig = new ExecutionConfig(1, 0, 5, TimeUnit.MINUTES);
        }
        context.setStatus("Executing " + jobTemplates.size() + " jobs using execution config: " + executionConfig);
        context.setTotalExpected(jobTemplates.size());

        List<JobExecutionTask> tasks = new ArrayList<>();
        for (JsonNode jobTemplate : jobTemplates) {
            PetlJobConfig petlJobConfig = context.getNestedJobConfig(jobTemplate, config.getVariables());
            tasks.add(new JobExecutionTask(petlJobConfig, context, executionConfig));
        }
        JobExecutor.execute(tasks, context, executionConfig);
    }
}
