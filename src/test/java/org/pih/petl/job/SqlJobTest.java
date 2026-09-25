package org.pih.petl.job;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
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
        executeJob("multipleScripts.yml");
        verifyTableExists("table1");
        verifyTableExists("table2");
    }

    @Test
    public void shouldPassParametersIntoScripts() throws Exception {
        verifyNoTablesExist();
        executeJob("parameterizedScript.yml");
        verifyTableExists("table3");
    }

    @Test
    public void shouldIdentifyFailingStatementInErrorMessage() throws Exception {
        verifyNoTablesExist();
        Exception e = executeJobAndReturnException("failingScript.yml");
        Assert.assertNotNull(e);
        String message = null;
        for (Throwable t : ExceptionUtils.getThrowables(e)) {
            if (t.getMessage() != null && t.getMessage().startsWith("Error in statement")) {
                message = t.getMessage();
            }
        }
        Assert.assertNotNull(message);
        Assert.assertTrue(message, message.startsWith("Error in statement 2 of "));
        Assert.assertTrue(message, message.contains("failingStatement.sql: insert into table_that_does_not_exist (id) values (1)"));
    }

    @Test
    public void shouldUseDelimiterToExecuteMultipleScripts() throws Exception {
        verifyNoTablesExist();
        executeJob("delimitedScript.yml");
    }
}
