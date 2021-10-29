package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.ExecutionConfig;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

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
        JobConfigReader configReader = new JobConfigReader(context);

        ExecutionConfig executionConfig = configReader.getObject(ExecutionConfig.class, "execution");
        if (executionConfig == null) {
            executionConfig = new ExecutionConfig(1, 0, 5, TimeUnit.MINUTES);
        }

        List<JsonNode> jobTemplates = configReader.getList("jobs");
        context.setStatus("Executing " + jobTemplates.size() + " jobs using execution config: " + executionConfig);

        List<JobExecutionTask> tasks = new ArrayList<>();
        for (JsonNode jobTemplate : jobTemplates) {
            JobConfig jobConfig = configReader.getJobConfig(jobTemplate);
            tasks.add(new JobExecutionTask(jobConfig, context, executionConfig));
        }
        JobExecutor.execute(tasks, context, executionConfig);
    }
}
