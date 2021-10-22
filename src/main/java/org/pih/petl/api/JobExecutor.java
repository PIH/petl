package org.pih.petl.api;

import org.hibernate.query.criteria.internal.expression.function.AggregationFunction;
import org.pih.petl.job.config.ExecutionConfig;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Responsible for executing jobs using ExecutorService
 */
public class JobExecutor {

    public static final Integer MAX_ALLOWED_THREADS = 10;

    public static void execute(List<JobExecutionTask> tasks, ExecutionContext context, ExecutionConfig config) throws InterruptedException, ExecutionException {
        context.setStatus("Executing tasks");

        int maxJobs = config.getMaxConcurrentJobs() == null ? 1 : (config.getMaxConcurrentJobs() < 0 ? MAX_ALLOWED_THREADS : config.getMaxConcurrentJobs());

        if (maxJobs != 0) {
            if (maxJobs == 1) {
                // If only one job should be executed at a time, then this means we want to process in serial, not parallel,
                // and we want to process each task and block on executing others unless tasks before execute successfully without exception
                for (JobExecutionTask task : tasks) {
                    Future<JobExecutionResult> resultFuture = Executors.newSingleThreadExecutor().submit(task);
                    JobExecutionResult result = resultFuture.get(); // This will throw an exception if one thrown in the executed task
                    if (!result.isSuccessful()) {
                        throw new RuntimeException("An error occurred executing a job", result.getException());
                    }
                }
            } else {
                // Otherwise, if more than one job is configured to execute at a time, assume this means we want to execute in parallel
                // And continue processing other jobs even if one or more of the jobs fail
                ExecutorService executorService = Executors.newFixedThreadPool(config.getMaxConcurrentJobs());
                List<Future<JobExecutionResult>> results = executorService.invokeAll(tasks);
                executorService.shutdown();

                // We only arrive here once all Tasks have been completed

                for (Future<JobExecutionResult> resultFutures : results) {
                    JobExecutionResult result = resultFutures.get();
                    if (!result.isSuccessful()) {
                        throw new RuntimeException("Error in one of the jobs", result.getException());
                    }
                }
            }
        }
        else {
            context.setStatus("Not executing job since it is configured with maxConcurrentJobs=0");
        }
    }

}
