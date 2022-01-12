package org.pih.petl.api;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Core service methods for loading jobs, executing jobs, and tracking the status of job executions
 */
@Service
public class EtlService {

    private static final Log log = LogFactory.getLog(EtlService.class);

    final ApplicationConfig applicationConfig;
    final JobExecutionRepository jobExecutionRepository;
    final JobExecutor jobExecutor;

    @Autowired
    List<PetlJob> petlJobs;

    @Autowired
    public EtlService(
            ApplicationConfig applicationConfig,
            JobExecutionRepository jobExecutionRepository
    ) {
        this.applicationConfig = applicationConfig;
        this.jobExecutionRepository = jobExecutionRepository;
        this.jobExecutor = new JobExecutor(applicationConfig.getPetlConfig().getMaxConcurrentJobs());
    }

    /**
     * This method will attempt to auto-detect and load all job configurations from the job directory
     * It will do this by iterating over all of the files recursively, retrieving any files it finds with a .yml or .yaml
     * extension, and checking if they represent valid job configuration files.  Each valid job configuration
     * will be returned in a Map keyed off of the job config path, relative to the configuration directory
     * eg. A job at ${PETL_JOB_DIR}/maternalhealth/deliveries.yml will be keyed at "maternalhealth/deliveries.yml"
     */
    public Map<String, JobConfig> getAllConfiguredJobs() {
        Map<String, JobConfig> m = new TreeMap<>();
        File jobDir = applicationConfig.getJobDir();
        if (jobDir != null) {
            final Path configPath = jobDir.toPath();
            log.trace("Loading configured jobs from: " + configPath);
            try {
                Files.walkFileTree(configPath, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) {
                        if (FilenameUtils.isExtension(path.toString().toLowerCase(), new String[] { "yml", "yaml" })) {
                            String relativePath = configPath.relativize(path).toString();
                            try {
                                JobConfig jobConfig = applicationConfig.getPetlJobConfig(relativePath);
                                PetlJob petlJob = getPetlJob(jobConfig);
                                if (petlJob == null) {
                                    throw new PetlException("Invalid job type specified: " + jobConfig.getType());
                                }
                                m.put(relativePath, jobConfig);
                            }
                            catch (Exception e) {
                                log.debug("Unable to load job config from file: " + relativePath, e);
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
            log.warn("No Job Directory configured, not returning any available jobs");
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

    public PetlJob getPetlJob(JobConfig jobConfig) {
        for (PetlJob petlJob : petlJobs) {
            Component component = petlJob.getClass().getAnnotation(Component.class);
            if (component != null && jobConfig.getType().equals(component.value())) {
                return petlJob;
            }
        }
        throw new PetlException("Unknown job type: " + jobConfig.getType());
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
            jobExecution.setStatus(JobExecutionStatus.ABORTED);
            jobExecutionRepository.save(jobExecution);
        }
    }

    /**
     * Executes the given job, returning the relevant job execution that contains status of the job
     */
    public JobExecution executeJob(String jobPath) {
        JobConfig jobConfig = applicationConfig.getPetlJobConfig(jobPath);
        JobExecution execution = new JobExecution(jobPath);
        log.info("Executing Job: " + execution);
        try {
            saveJobExecution(execution);
            PetlJob petlJob = getPetlJob(jobConfig);
            ExecutionContext context = new ExecutionContext(execution, jobConfig, applicationConfig);
            jobExecutor.execute(new JobExecutionTask(petlJob, context));
            execution.setStatus(JobExecutionStatus.SUCCEEDED);
            log.info("Job Successful: " + execution);
        }
        catch (Throwable t) {
            String exception = ExceptionUtils.getMessage(t);
            execution.setErrorMessage(exception.substring(0,1000));
            execution.setStatus(JobExecutionStatus.FAILED);
			log.error("Job Execution Failed for " + jobPath, t);
            throw(new PetlException("Job Execution Failed for " + jobPath, t));
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
