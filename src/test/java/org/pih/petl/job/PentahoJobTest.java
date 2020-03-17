package org.pih.petl.job;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PentahoJobTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void testSimpleJobThatOutputsLoggingMessage() {
        etlService.executeJob("pentaho/job.yml");
    }
}
