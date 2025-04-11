package org.pih.petl.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.dockerjava.api.model.Container;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.DockerConnector;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
import org.pih.petl.api.JobExecutionTask;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.DataSource;
import org.pih.petl.job.config.JobConfig;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        List<String> containersStarted = new ArrayList<>();
        try {
            startContainersIfNecessary(containersStarted, configReader);
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
            DockerConnector.stopContainers(containersStarted);
            jobExecutor.shutdown();
        }
    }

    /**
     * starts any containers referenced in datasources that are not already started
     * @return a list of container names that were newly started
     */
    private void startContainersIfNecessary(List<String> containersStarted, JobConfigReader configReader) {
        for (DataSource dataSource : configReader.getDataSources("datasources")) {
            if (dataSource.startContainerIfNecessary()) {
                containersStarted.add(dataSource.getContainerName());
            }
        }
    }
}
