package org.pih.petl.job;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/runmultiple"})
public class RunMultipleTest extends BasePetlTest {

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return Collections.singletonList("encounter_types");
    }

    @Test
    public void testJobWithSqlExecution() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("defaultExecution.yml");
        verifyAllTablesExist();
        verifyRowCount("encounter_types", 62);
    }
}
