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
            containersStarted = startContainersIfNecessary(configReader);
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
            stopContainers(containersStarted);
            jobExecutor.shutdown();
        }
    }

    /**
     * starts any containers referenced in datasources that are not already started
     * @return a list of container names that were newly started
     */
    private List<String> startContainersIfNecessary(JobConfigReader configReader) {
        List<String> ret = new ArrayList<>();
        for (DataSource dataSource : configReader.getDataSources("datasources")) {
            String containerName = dataSource.getContainerName();
            if (StringUtils.isNotBlank(containerName)) {
                log.info("Checking if container '" + containerName + "' is started");
                try (DockerConnector docker = DockerConnector.open()) {
                    Container container = docker.getContainer(containerName);
                    if (container != null) {
                        if (docker.isContainerRunning(container)) {
                            log.info("Container '" + containerName + "' is already running");
                        }
                        else {
                            log.info("Container '" + containerName + "' is not already running, starting it");
                            docker.startContainer(container);
                            ret.add(containerName);
                            log.info("Container started");
                        }
                        log.info("Testing for a successful database connection to  '" + containerName + "'");
                        // Wait up to 1 minute for the container to return a valid connection
                        int numSecondsToWait = 60;
                        while (numSecondsToWait >= 0) {
                            log.info("Waiting for connection for " + numSecondsToWait + " seconds");
                            numSecondsToWait--;
                            Exception exception = null;
                            try {
                                if (dataSource.testConnection()) {
                                    break;
                                }
                            }
                            catch (Exception e) {
                                exception = e;
                            }
                            if (numSecondsToWait == 0) {
                                throw new RuntimeException("Could not establish database connection to container " + containerName, exception);
                            }
                        }
                    }
                    else {
                        log.warn("No container named " + containerName + " found, skipping");
                    }
                }
            }
        }
        return ret;
    }

    private void stopContainers(List<String> containersToStop) {
        if (containersToStop != null) {
            for (String containerName : containersToStop) {
                log.info("Stopping previously started container " + containerName);
                try (DockerConnector docker = DockerConnector.open()) {
                    Container container = docker.getContainer(containerName);
                    if (container != null) {
                        if (docker.isContainerRunning(container)) {
                            docker.stopContainer(container);
                            log.info("Container '" + containerName + "' stopped");
                        }
                        else {
                            log.info("Container '" + containerName + "' is not running");
                        }
                    }
                }
            }
        }
    }
}
