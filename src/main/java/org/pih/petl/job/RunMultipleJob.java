package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
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

    private final Log log = LogFactory.getLog(getClass());

    @Autowired
    EtlService etlService;

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final JobExecution jobExecution) throws Exception {
        JobConfigReader configReader = new JobConfigReader(etlService.getApplicationConfig(), jobExecution.getJobConfig());
        JobExecutor jobExecutor = new JobExecutor(etlService, 1);
        try {
            List<JsonNode> jobTemplates = configReader.getList("jobs");
            log.debug("Executing " + jobTemplates.size() + " jobs");
            List<JobExecutionTask> tasks = new ArrayList<>();
            int sequenceNum = 1;
            for (JsonNode jobTemplate : jobTemplates) {
                JobConfig childConfig = configReader.getJobConfig(jobTemplate);
                JobExecution childExecution = new JobExecution(jobExecution, childConfig, sequenceNum++);
                tasks.add(new JobExecutionTask(etlService, childExecution));
            }
            jobExecutor.executeInSeries(tasks);
        }
        finally {
            jobExecutor.shutdown();
        }
    }
}
