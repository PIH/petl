package org.pih.petl;

import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfigReader;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("test-job")
public class TestJob implements PetlJob {

    public static final Map<String, Integer> attemptNum = new HashMap<>();
    public static final Map<String, Boolean> successful = new HashMap<>();
    public static final List<String> jobsCompleted = new ArrayList<>();

    public static void clearResults(String testId) {
        attemptNum.remove(testId);
        successful.remove(testId);
        jobsCompleted.remove(testId);
    }

    @Override
    public void execute(ExecutionContext context) throws Exception {
        JobConfigReader jobConfigReader = new JobConfigReader(context.getApplicationConfig(), context.getJobConfig());
        String testId = jobConfigReader.getString("testId");
        successful.put(testId, false);
        int currentAttemptNum = (attemptNum.get(testId) == null ? 1 : attemptNum.get(testId) + 1);
        attemptNum.put(testId, currentAttemptNum);

        int simulateJobDurationSeconds = jobConfigReader.getInt(0,"simulateJobDurationSeconds");
        if (simulateJobDurationSeconds > 0) {
            TimeUnit.SECONDS.sleep(simulateJobDurationSeconds);
        }

        boolean simulateFailure = jobConfigReader.getBoolean(false, "simulateFailure");
        if (simulateFailure) {
            throw new PetlException("Simulated failure for job");
        }

        int simulateSuccessOnAttempt = jobConfigReader.getInt(1, "simulateSuccessOnAttempt");
        if (simulateSuccessOnAttempt > currentAttemptNum) {
            throw new PetlException("Simulated failure for job on attempt " + currentAttemptNum);
        }

        successful.put(testId, true);
        jobsCompleted.add(testId);
    }
}
