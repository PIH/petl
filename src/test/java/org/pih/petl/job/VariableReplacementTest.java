package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.JobConfig;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/variablereplacement"})
public class VariableReplacementTest extends BasePetlTest {

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return new ArrayList<>();
    }

    @Test
    public void shouldExecuteAllPassedScripts() throws Exception {
        JobConfig jobConfig = etlService.getAllConfiguredJobs().get("jobWithSchedule.yml");
        Assert.assertNotNull(jobConfig);
        Assert.assertEquals("0 15 9 ? * *", jobConfig.getSchedule().getCron());
    }
}
