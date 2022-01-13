package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.api.JobExecution;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a particular ETL job configuration
 */
@Component("iterating-job")
public class IteratingJob implements PetlJob {

    @Autowired
    EtlService etlService;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {
        context.setStatus("Executing IteratingJob");
        JobConfigReader configReader = new JobConfigReader(etlService.getApplicationConfig(), context.getJobConfig());
        JobExecutor jobExecutor = new JobExecutor(etlService, configReader.getInt(1, "maxConcurrentJobs"));
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
                PetlJob petlJob = etlService.getPetlJob(childConfig);
                JobExecution childExecution = new JobExecution(null, context.getJobExecution().getUuid(), childConfig.getDescription());
                ExecutionContext iterationContext = new ExecutionContext(childExecution, childConfig);
                iterationTasks.add(new JobExecutionTask(etlService, petlJob, iterationContext));
                context.setStatus("Adding iteration task: " + iterationVars);
            }
            jobExecutor.executeInParallel(iterationTasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
