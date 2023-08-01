package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.job.config.ErrorHandling;
import org.pih.petl.job.config.JobConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Responsible for executing jobs using ExecutorService
 */
public class JobExecutor {

    private static Log log = LogFactory.getLog(JobExecutor.class);

    private final EtlService etlService;
    private final ScheduledExecutorService executorService;

    public JobExecutor(EtlService etlService, Integer maxConcurrentJobs) {
        this.etlService = etlService;
        this.executorService = Executors.newScheduledThreadPool(maxConcurrentJobs);
    }

    public void shutdown() {
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }
    }

    /**
     * Executes the given job, returning the relevant job execution that contains status of the job
     * This is intended to be how all top-level jobs are executed, whether executed at startup, or
     * via a scheduled execution.
     */
    public JobExecution executeJob(String jobPath) {
        JobConfig jobConfig = etlService.getApplicationConfig().getPetlJobConfig(jobPath);
        JobExecution execution = new JobExecution(jobPath, jobConfig);
        execution.setDescription(jobConfig.getDescription());
        etlService.saveJobExecution(execution);
        return executeJob(execution);
    }

    /**
     * Executes the given job, returning the relevant job execution that contains status of the job
     */
    public JobExecution executeJob(JobExecution execution) {
        try {
            log.info(execution);
            executeInSeries(Collections.singletonList(new JobExecutionTask(etlService, execution)));
            execution.setStatus(JobExecutionStatus.SUCCEEDED);
            execution.setErrorMessage(null);
        }
        catch (Throwable t) {
            execution.setErrorMessageFromException(t);
            execution.setStatus(JobExecutionStatus.FAILED);
            log.error(execution, t);
            throw(new PetlException("Job Execution Failed: " + execution, t));
        }
        finally {
            execution.setCompleted(new Date());
            etlService.saveJobExecution(execution);
            log.info(execution);
        }
        return execution;
    }

    /**
     * Execute a List of jobs in parallel.
     */
    public void executeInParallel(List<JobExecutionTask> tasks) throws InterruptedException, ExecutionException {
        List<JobExecutionResult> finalResults = new ArrayList<>();
        List<JobExecutionTask> tasksToSchedule = new ArrayList<>(tasks);
        log.debug("Executing " + tasksToSchedule.size() + " tasks in parallel");
        while (tasksToSchedule.size() > 0) {
            List<Future<JobExecutionResult>> futures = new ArrayList<>();
            for (JobExecutionTask task : tasksToSchedule) {
                JobExecution execution = task.getJobExecution();
                etlService.saveJobExecution(execution);
                if (task.getAttemptNum() == 1) {
                    log.debug("First attempt at " + task + " submitting to executor service for execution");
                    futures.add(executorService.submit(task));
                    execution.setStatus(JobExecutionStatus.QUEUED);
                    etlService.saveJobExecution(execution);
                    log.info(execution);
                }
                else {
                    ErrorHandling errorHandling = task.getJobExecution().getJobConfig().getErrorHandling();
                    log.debug("Retry attempt at " + task + ", scheduling for re-execution: " + errorHandling);
                    futures.add(executorService.schedule(task, errorHandling.getRetryInterval(), errorHandling.getRetryIntervalUnit()));
                    execution.setStatus(JobExecutionStatus.RETRY_QUEUED);
                    etlService.saveJobExecution(execution);
                    log.info(execution);
                }
            }
            for (Future<JobExecutionResult> future : futures) {
                JobExecutionResult result = future.get();
                JobExecutionTask task = result.getJobExecutionTask();
                JobExecution execution = task.getJobExecution();
                int maxAttempts = task.getJobExecution().getJobConfig().getErrorHandling().getMaxAttempts();
                if (result.isSuccessful() || task.getAttemptNum() >= maxAttempts) {
                    finalResults.add(result);
                    tasksToSchedule.remove(task);
                    execution.setCompleted(new Date());
                    if (result.isSuccessful()) {
                        execution.setStatus(JobExecutionStatus.SUCCEEDED);
                    } else {
                        execution.setStatus(JobExecutionStatus.FAILED);
                        execution.setErrorMessageFromException(result.getException());
                    }
                }
                else {
                    log.info("Task failed, but will retry.  Attempt: " + task.getAttemptNum() + "; max attempts: " + maxAttempts);
                    task.incrementAttemptNum();
                    execution.setStatus(JobExecutionStatus.FAILED_WILL_RETRY);
                    execution.setErrorMessageFromException(result.getException());
                }
                etlService.saveJobExecution(execution);
                log.info(execution);
            }
        }
        List<Throwable> errors = new ArrayList<>();
        for (JobExecutionResult finalResult : finalResults) {
            if (!finalResult.isSuccessful()) {
                errors.add(finalResult.getException());
            }
        }
        if (errors.size() > 0) {
            throw new PetlException("Errors occurred in " + errors.size() + " / " + finalResults.size() + " jobs");
        }
    }

    /**
     * Execute a List of jobs in series.  A failure will terminate immediately and subsequent jobs will not run
     */
    public void executeInSeries(List<JobExecutionTask> tasks) throws InterruptedException, ExecutionException {

        log.debug("Executing " + tasks.size() + " tasks in series");

        // First, ensure all job executions are saved so that they can be tracked and re-initiated as needed
        for (JobExecutionTask task : tasks) {
            JobExecution execution = task.getJobExecution();
            etlService.saveJobExecution(execution);
        }

        // Next, execute each and only execute subsequent tasks if the earlier ones are successful
        JobExecutionResult failedResult = null;
        for (JobExecutionTask task : tasks) {
            JobExecution execution = task.getJobExecution();
            etlService.saveJobExecution(execution);
            if (failedResult == null) {
                Future<JobExecutionResult> futureResult = executorService.submit(task);
                execution.setStatus(JobExecutionStatus.QUEUED);
                etlService.saveJobExecution(execution);
                log.info(execution);

                JobExecutionResult result = futureResult.get(); // This blocks until result is available
                ErrorHandling errorHandling = task.getJobExecution().getJobConfig().getErrorHandling();
                while (!result.isSuccessful() && task.getAttemptNum() < errorHandling.getMaxAttempts()) {
                    log.info("Task failed, but will retry.  Attempt: " + task.getAttemptNum() + "; max attempts: " + errorHandling.getMaxAttempts());
                    task.incrementAttemptNum();
                    execution.setStatus(JobExecutionStatus.RETRY_QUEUED);
                    etlService.saveJobExecution(execution);
                    log.info(execution);
                    result = executorService.schedule(task, errorHandling.getRetryInterval(), errorHandling.getRetryIntervalUnit()).get();
                }
                execution.setCompleted(new Date());
                if (result.isSuccessful()) {
                    execution.setStatus(JobExecutionStatus.SUCCEEDED);
                } else {
                    execution.setStatus(JobExecutionStatus.FAILED);
                    execution.setErrorMessageFromException(result.getException());
                    failedResult = result;
                }
            }
            else {
                execution.setCompleted(new Date());
                execution.setStatus(JobExecutionStatus.ABORTED);
            }
            etlService.saveJobExecution(execution);
            log.info(execution);
        }
        if (failedResult != null) {
            throw new PetlException("An error occurred executing one or more jobs", failedResult.getException());
        }
    }
}
