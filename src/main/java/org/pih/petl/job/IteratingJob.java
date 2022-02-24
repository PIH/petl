package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private final Log log = LogFactory.getLog(getClass());

    @Autowired
    EtlService etlService;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {
        log.debug("Executing IteratingJob");
        JobConfigReader configReader = new JobConfigReader(etlService.getApplicationConfig(), jobExecution.getJobConfig());
        JobExecutor jobExecutor = new JobExecutor(etlService, configReader.getInt(1, "maxConcurrentJobs"));
        try {
            List<JsonNode> iterations = configReader.getList("iterations");
            List<JobExecutionTask> iterationTasks = new ArrayList<>();
            int sequenceNum = 1;
            for (JsonNode iteration : iterations) {
                Map<String, String> iterationVars = configReader.getMap(iteration);
                for (String paramName : iterationVars.keySet()) {
                    String paramValue = iterationVars.get(paramName);
                    iterationVars.put(paramName, StrSubstitutor.replace(paramValue, jobExecution.getJobConfig().getParameters()));
                }
                JobConfig childConfig = configReader.getJobConfig(iterationVars, "jobTemplate");
                JobExecution childExecution = new JobExecution(jobExecution, childConfig, sequenceNum++);
                iterationTasks.add(new JobExecutionTask(etlService, childExecution));
                log.debug("Adding iteration task: " + iterationVars);
            }
            jobExecutor.executeInParallel(iterationTasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
