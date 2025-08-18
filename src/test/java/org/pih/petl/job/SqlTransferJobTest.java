package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/sqltransfer"})
public class SqlTransferJobTest extends BasePetlTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return Collections.singletonList("encounter_types");
    }

    @Test
    public void testJob() throws Exception {
        executeJob("job.yml");
        Assert.assertTrue(getTargetMySQLDatasource().tableExists("encounter_types"));
        Assert.assertEquals(62, getTargetMySQLDatasource().rowCount("encounter_types"));

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        executeJob("job.yml");
        Assert.assertTrue(getTargetMySQLDatasource().tableExists("encounter_types"));
        Assert.assertEquals(62, getTargetMySQLDatasource().rowCount("encounter_types"));
    }
}
