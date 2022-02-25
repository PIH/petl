package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.job.config.TableColumn;
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
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/createtable"})
public class CreateTableJobTest extends BasePetlTest {

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Override
    List<String> getTablesCreated() {
        return Collections.singletonList("encounter_types");
    }

    @Test
    public void testLoadingFromMySQL() throws Exception {
        verifyNoTablesExist();
        executeJob("createEncounterTypesFromSchema.yml");
        verifyAllTablesExist();
    }

    @Test
    public void testLoadingFromMySQLDropIfExists() throws Exception {
        verifyNoTablesExist();
        executeJob("createEncounterTypesDrop.yml");
        verifyAllTablesExist();
    }

    @Test
    public void testLoadingFromSchemaDropIfSchemaChanged() throws Exception {
        verifyNoTablesExist();
        executeJob("createEncounterTypesDropIfChanged.yml");
        verifyAllTablesExist();
        List<TableColumn> columns = getSqlServerDatasource().getTableColumns("encounter_types");
        Assert.assertEquals(4, columns.size());
        Assert.assertEquals("UUID CHAR(38)", columns.get(0).toString().toUpperCase());
        Assert.assertEquals("NAME VARCHAR(100)", columns.get(1).toString().toUpperCase());
        Assert.assertEquals("DESCRIPTION VARCHAR(1000)", columns.get(2).toString().toUpperCase());
        Assert.assertEquals("PROVIDER VARCHAR(100)", columns.get(3).toString().toUpperCase());
    }
}
