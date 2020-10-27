package org.pih.petl.api;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.PetlJobConfig;
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
        Map<String, PetlJobConfig> configuredJobs = etlService.getAllConfiguredJobs();
        Assert.assertEquals(5, configuredJobs.size());
        Assert.assertNotNull(configuredJobs.get("sqlserverimport/job.yml"));
        Assert.assertNotNull(configuredJobs.get("pentaho/job.yml"));
    }
}
