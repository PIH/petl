package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates a particular ETL job configuration
 */
@Component("job-pipeline")
public class RunMultipleJob implements PetlJob {

    @Autowired
    EtlService etlService;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        JobConfigReader configReader = new JobConfigReader(context);
        JobExecutor jobExecutor = new JobExecutor(1);
        try {
            List<JsonNode> jobTemplates = configReader.getList("jobs");
            context.setStatus("Executing " + jobTemplates.size() + " jobs");
            List<JobExecutionTask> tasks = new ArrayList<>();
            for (JsonNode jobTemplate : jobTemplates) {
                JobConfig childConfig = configReader.getJobConfig(jobTemplate);
                PetlJob petlJob = etlService.getPetlJob(childConfig);
                ExecutionContext iterationContext = new ExecutionContext(context, childConfig);
                tasks.add(new JobExecutionTask(petlJob, iterationContext));
            }
            jobExecutor.executeInSeries(tasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
