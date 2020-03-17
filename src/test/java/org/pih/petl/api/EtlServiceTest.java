package org.pih.petl.api;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.PetlJobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EtlServiceTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupPetlHome();
    }

    @Test
    public void testServiceLoadsAllConfiguredJobs() {
        Map<String, PetlJobConfig> configuredJobs = etlService.getAllConfiguredJobs();
        Assert.assertEquals(2, configuredJobs.size());
        Assert.assertNotNull(configuredJobs.get("jobs/sqlserverimport/job.yml"));
        Assert.assertNotNull(configuredJobs.get("jobs/pentaho/job.yml"));
    }
}
