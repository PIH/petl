package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates a particular ETL job configuration
 */
public class RunMultipleJob implements PetlJob {

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        JobConfigReader configReader = new JobConfigReader(context);
        JobExecutor jobExecutor = new JobExecutor(configReader.getInt(1, "maxConcurrentJobs"));
        try {
            List<JsonNode> jobTemplates = configReader.getList("jobs");
            context.setStatus("Executing " + jobTemplates.size() + " jobs");
            List<JobExecutionTask> tasks = new ArrayList<>();
            for (JsonNode jobTemplate : jobTemplates) {
                JobConfig childJobConfig = configReader.getJobConfig(jobTemplate);
                tasks.add(new JobExecutionTask(new ExecutionContext(context, childJobConfig)));
            }
            jobExecutor.execute(tasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
