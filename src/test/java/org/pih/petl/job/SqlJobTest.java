package org.pih.petl.job;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/sql"})
public class SqlJobTest extends BasePetlTest {

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return Arrays.asList("table1", "table2", "table3");
    }

    @Test
    public void shouldExecuteAllPassedScripts() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("multipleScripts.yml");
        verifyTableExists("table1");
        verifyTableExists("table2");
    }

    @Test
    public void shouldPassParametersIntoScripts() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("parameterizedScript.yml");
        verifyTableExists("table3");
    }

    @Test
    public void shouldUseDelimiterToExecuteMultipleScripts() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("delimitedScript.yml");
    }
}
