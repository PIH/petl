package org.pih.petl.api;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Saves job executions from several threads at once, as parent and child jobs do, with a config too large
 * to be stored in the row. H2 1.4.199 failed this intermittently with a NullPointerException in
 * LobStorageMap.copyLob (H2 issue #1808).
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class JobExecutionConcurrentSaveTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void shouldSaveJobExecutionsConcurrently() throws Exception {
        // The failure was intermittent, so the scenario runs several times
        for (int round = 0; round < 20; round++) {
            saveConcurrently(16, 200);
        }
    }

    private void saveConcurrently(int threads, int savesPerThread) throws Exception {
        String config = StringUtils.repeat("configuration: value\n", 500);
        List<String> uuids = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<JobExecution>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(executor.submit(() -> {
                    JobExecution execution = new JobExecution();
                    execution.setInitiated(new Date());
                    execution.setConfig(config);
                    uuids.add(execution.getUuid());
                    for (int j = 0; j < savesPerThread; j++) {
                        execution.setStatus(j % 2 == 0 ? JobExecutionStatus.IN_PROGRESS : JobExecutionStatus.SUCCEEDED);
                        etlService.saveJobExecution(execution);
                        // As a parent reads its children's executions while they save
                        etlService.getJobExecution(uuids.get(j % uuids.size()));
                    }
                    return execution;
                }));
            }
            for (Future<JobExecution> result : results) {
                JobExecution saved = etlService.getJobExecution(result.get(2, TimeUnit.MINUTES).getUuid());
                Assert.assertEquals(JobExecutionStatus.SUCCEEDED, saved.getStatus());
                Assert.assertEquals(config, saved.getConfig());
            }
        }
        finally {
            executor.shutdownNow();
        }
    }
}
