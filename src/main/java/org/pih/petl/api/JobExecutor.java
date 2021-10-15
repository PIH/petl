package org.pih.petl.api;

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

    public static void execute(List<JobExecutionTask> tasks, ExecutionContext context, ExecutionConfig config) throws InterruptedException, ExecutionException {
        context.setStatus("Executing tasks");
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
