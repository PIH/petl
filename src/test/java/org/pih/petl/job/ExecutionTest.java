package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.TestJob;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/execution"})
public class ExecutionTest extends BasePetlTest {

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return new ArrayList<>();
    }

    @Test
    public void testFailAfterMaxAttempts() {
        String testId = "testFailAfterMaxAttempts";
        Exception exception = executeJob("testMaxAttempts.yml");
        Assert.assertNotNull(exception);
        Assert.assertEquals(10, TestJob.attemptNum.get(testId).intValue());
        Assert.assertFalse(TestJob.successful.get(testId));
        TestJob.clearResults(testId);
    }

    @Test
    public void testSucceedAfterFourAttempts() {
        String testId = "testSucceedAfterFourAttempts";
        Exception exception = executeJob("testFourAttempts.yml");
        Assert.assertNull(exception);
        Assert.assertEquals(4, TestJob.attemptNum.get(testId).intValue());
        Assert.assertTrue(TestJob.successful.get(testId));
        TestJob.clearResults(testId);
    }

    @Test
    public void testSerialExecution() throws Exception {
        Exception exception = executeJob("serialExecution.yml");
        Assert.assertNull(exception);
        Assert.assertEquals(3, TestJob.jobsCompleted.size());
        Assert.assertEquals("serialJob1", TestJob.jobsCompleted.get(0));
        Assert.assertEquals("serialJob2", TestJob.jobsCompleted.get(1));
        Assert.assertEquals("serialJob3", TestJob.jobsCompleted.get(2));
        TestJob.jobsCompleted.clear();
    }

    @Test
    public void testParallelExecution() throws Exception {
        Exception exception = executeJob("parallelExecution.yml");
        Assert.assertNull(exception);
        Assert.assertEquals(3, TestJob.jobsCompleted.size());
        Assert.assertEquals("parallelJob2", TestJob.jobsCompleted.get(0));
        Assert.assertEquals("parallelJob3", TestJob.jobsCompleted.get(1));
        Assert.assertEquals("parallelJob1", TestJob.jobsCompleted.get(2));
        TestJob.jobsCompleted.clear();
    }

    public Exception executeJob(String jobFile) {
        Exception exception = null;
        try {
            etlService.executeJob(jobFile);
        }
        catch (Exception e) {
            exception = e;
        }
        return exception;
    }
}
