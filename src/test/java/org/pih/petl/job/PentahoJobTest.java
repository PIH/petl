package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
import org.pih.petl.api.JobExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/pentaho"})
public class PentahoJobTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void testSimpleJobThatOutputsLoggingMessage() {
        JobExecutor executor = new JobExecutor(etlService, 1);
        JobExecution execution = executor.executeJob("job.yml");
        Assert.assertNull(execution.getErrorMessage());
    }

    @Test
    public void testJobThatFailsResultsInAFailedPetlJobExecution() {
        JobExecutor executor = new JobExecutor(etlService, 1);
        Throwable exception = null;
        try {
            executor.executeJob("failing-job.yml");
        }
        catch (Throwable t) {
            exception = t;
        }
        Assert.assertNotNull(exception);

    }
}
