package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
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
        etlService.executeJob("createEncounterTypesFromSchema.yml");
        verifyAllTablesExist();
    }

    @Test
    public void testLoadingFromMySQLDropIfExists() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("createEncounterTypesDrop.yml");
        verifyAllTablesExist();
    }

    @Test
    public void testLoadingFromSchemaDropIfSchemaChanged() throws Exception {
        verifyNoTablesExist();
        etlService.executeJob("createEncounterTypesDropIfChanged.yml");
        verifyAllTablesExist();
        Assert.assertEquals(4, getSqlServerDatasource().getTableColumns("encounter_types").size());
    }
}
