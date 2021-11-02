package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.job.config.ErrorHandling;

import java.util.ArrayList;
import java.util.Collections;
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

    private final Integer maxConcurrentJobs;
    private final ScheduledExecutorService executorService;

    public JobExecutor(Integer maxConcurrentJobs) {
        this.maxConcurrentJobs = maxConcurrentJobs;
        this.executorService = Executors.newScheduledThreadPool(maxConcurrentJobs);
    }


    /**
     * Execute a single task
     */
    public void execute(List<JobExecutionTask> tasks) throws InterruptedException, ExecutionException {
        if (maxConcurrentJobs == 1) {
            executeInSeries(tasks);
        }
        else {
            executeInParallel(tasks);
        }
    }

    /**
     * Execute a single task
     */
    public void execute(JobExecutionTask task) throws InterruptedException, ExecutionException {
        executeInSeries(Collections.singletonList(task));
    }

    /**
     * Execute a List of jobs in parallel.
     */
    public void executeInParallel(List<JobExecutionTask> tasks) throws InterruptedException, ExecutionException {
        List<JobExecutionResult> finalResults = new ArrayList<>();
        List<JobExecutionTask> tasksToSchedule = new ArrayList<>(tasks);
        while (tasksToSchedule.size() > 0) {
            List<Future<JobExecutionResult>> futures = new ArrayList<>();
            for (JobExecutionTask task : tasksToSchedule) {
                if (task.getAttemptNum() == 1) {
                    futures.add(executorService.submit(task));
                    log.warn("Job " + task.getJobConfig() + " initial execution submitted");
                }
                else {
                    ErrorHandling errorHandling = task.getJobConfig().getErrorHandling();
                    futures.add(executorService.schedule(task, errorHandling.getRetryInterval(), errorHandling.getRetryIntervalUnit()));
                    log.warn("Job " + task.getJobConfig() + " retry execution scheduled: " + errorHandling);
                }
            }
            for (Future<JobExecutionResult> future : futures) {
                JobExecutionResult result = future.get();
                JobExecutionTask task = result.getJobExecutionTask();
                if (result.isSuccessful() || task.getAttemptNum() >= task.getJobConfig().getErrorHandling().getMaxAttempts()) {
                    finalResults.add(result);
                    tasksToSchedule.remove(task);
                }
                else {
                    task.incrementAttemptNum();
                }
            }
        }
        for (JobExecutionResult finalResult : finalResults) {
            List<Throwable> errors = new ArrayList<>();
            if (!finalResult.isSuccessful()) {
                errors.add(finalResult.getException());
            }
            if (errors.size() > 0) {
                log.error("There were errors in " + errors.size() + " / " + tasks);
                for (Throwable t : errors) {
                    log.error(t);
                }
                throw new PetlException("Error executing job");
            }
        }
    }

    /**
     * Execute a List of jobs in series.  There is no retrying of failed jobs with this method.
     * This is typically used by jobs that contain other jobs to run in parallel.  Retrying of failed jobs is done at the parent level.
     */
    public void executeInSeries(List<JobExecutionTask> tasks) throws InterruptedException, ExecutionException {
        for (JobExecutionTask task : tasks) {
            ErrorHandling errorHandling = task.getJobConfig().getErrorHandling();
            JobExecutionResult result = executorService.submit(task).get(); // This blocks until result is available
            while (!result.isSuccessful() && task.getAttemptNum() < errorHandling.getMaxAttempts()) {
                task.incrementAttemptNum();
                log.warn("Job " + task.getJobConfig() + " failed.  Reattempting with error handling: " + errorHandling);
                result = executorService.schedule(task, errorHandling.getRetryInterval(), errorHandling.getRetryIntervalUnit()).get();
            }
            if (!result.isSuccessful()) {
                throw new PetlException("Error executing job: " + task.getJobConfig(), result.getException());
            }
        }
    }
}
