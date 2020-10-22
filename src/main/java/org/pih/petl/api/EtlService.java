package org.pih.petl.api;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.PetlJobConfig;
import org.pih.petl.job.config.PetlJobFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Core service methods for loading jobs, executing jobs, and tracking the status of job executions
 */
@Service
public class EtlService {

    private static Log log = LogFactory.getLog(EtlService.class);

    @Autowired
    ApplicationConfig applicationConfig;

    @Autowired
    JobExecutionRepository jobExecutionRepository;

    /**
     * This method will attempt to auto-detect and load all job configurations from the job directory
     * It will do this by iterating over all of the files recursively, retrieving any files it finds with a .yml or .yaml
     * extension, and checking if they represent valid job configuration files.  Each valid job configuration
     * will be returned in a Map keyed off of the job config path, relative to the configuration directory
     * eg. A job at ${PETL_JOB_DIR}/maternalhealth/deliveries.yml will be keyed at "maternalhealth/deliveries.yml"
     */
    public Map<String, PetlJobConfig> getAllConfiguredJobs() {
        Map<String, PetlJobConfig> m = new TreeMap<>();
        File jobDir = applicationConfig.getJobDir();
        if (jobDir != null) {
            final Path configPath = jobDir.toPath();
            log.debug("Loading configured jobs from: " + configPath);
            try {
                Files.walkFileTree(configPath, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                        if (FilenameUtils.isExtension(path.toString().toLowerCase(), new String[] { "yml", "yaml" })) {
                            String relativePath = configPath.relativize(path).toString();
                            try {
                                PetlJobConfig jobConfig = applicationConfig.getPetlJobConfig(relativePath);
                                if (PetlJobFactory.isValid(jobConfig)) {
                                    m.put(relativePath, jobConfig);
                                }
                            }
                            catch (Exception e) {
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            catch (Exception e) {
                throw new PetlException("Error reading configuration files from " + configPath, e);
            }
        }
        else {
            log.debug("No Job Directory configured, not returning any available jobs");
        }
        return m;
    }

    /**
     * @return the most recent Job Execution for the given Job Path
     */
    public JobExecution getLatestJobExecution(String jobPath) {
        List<JobExecution> l = jobExecutionRepository.findJobExecutionByJobPathOrderByStartedDesc(jobPath);
        if (l == null || l.isEmpty()) {
            return null;
        }
        return l.get(0);
    }

    /**
     * Save the given job execution to the DB
     */
    @Transactional
    public JobExecution saveJobExecution(JobExecution jobExecution) {
        jobExecution = jobExecutionRepository.save(jobExecution);
        log.debug(jobExecution);
        return jobExecution;
    }

    /**
     * Update all jobs with null date completed to have date completed = NOW
     * (Used on startup to make sure hung jobs are rerun at next scheduled interval)
     */
    public void markHungJobsAsRun() {
        for (JobExecution jobExecution : jobExecutionRepository.findJobExecutionsByCompletedIsNullAndStartedIsNotNull()) {
            jobExecution.setCompleted(new Date());
            jobExecutionRepository.save(jobExecution);
        }
    }

    /**
     * Executes the given job, returning the relevant job execution that contains status of the job
     */
    public JobExecution executeJob(String jobPath) {
        PetlJobConfig jobConfig = applicationConfig.getPetlJobConfig(jobPath);
        PetlJob job = PetlJobFactory.instantiate(jobConfig);
        String executionUuid = UUID.randomUUID().toString();
        JobExecution execution = new JobExecution(executionUuid, jobPath);
        log.info("Executing Job: " + jobPath + " (" + executionUuid + ")");
        try {
            saveJobExecution(execution);
            ExecutionContext context = new ExecutionContext(execution, jobConfig, applicationConfig);
            job.execute(context);
            execution.setStatus("Execution Successful");
            log.info("Job Successful: " + jobPath + " (" + executionUuid + ")");
        }
        catch (Exception e) {
            execution.setErrorMessage(e.getMessage());
            execution.setStatus("Execution Failed");
            log.error("Error executing job: " + job, e);
        }
        finally {
            execution.setCompleted(new Date());
            saveJobExecution(execution);
        }
        return execution;
    }

    /**
     * Convenience method to allow access to the application configuration
     */
    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }
}
