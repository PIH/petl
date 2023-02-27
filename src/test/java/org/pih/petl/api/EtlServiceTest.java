package org.pih.petl.api;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.JobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EtlServiceTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void testServiceLoadsAllConfiguredJobs() {
        Map<String, JobConfig> configuredJobs = etlService.getAllConfiguredJobs();
        Assert.assertTrue(configuredJobs.size() > 13);
        Assert.assertNotNull(configuredJobs.get("sqlserverimport/job.yml"));
        Assert.assertNotNull(configuredJobs.get("pentaho/job.yml"));
    }

    @Test
    public void testSettingAndGettingState() {
        etlService.setStateValue("testJob", "stateKey", "stateValue");
        Assert.assertEquals(etlService.getStateValue("testJob", "stateKey"), "stateValue");
        Assert.assertNull(etlService.getStateValue("testJob", "otherKey"));
    }
}
