package org.pih.petl.job;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.datasource.DatabaseUtil;
import org.pih.petl.job.datasource.EtlDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/createtable"})
public class CreateTableJobTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @After
    public void dropTablesInTargetDB() throws Exception{
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            DatabaseUtil.dropTable(c, "encounter_types");
        }
    }

    @Test
    public void testLoadingFromMySQL() throws Exception {
        verifyTableDoesNotExist("encounter_types");
        etlService.executeJob("job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 0);
    }

    public void verifyRowCount(String table, int expectedRows) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            int rowsFound = DatabaseUtil.rowCount(c, table);
            Assert.assertEquals(expectedRows, rowsFound);
        }
    }

    public void verifyTableExists(String table) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            Assert.assertTrue(DatabaseUtil.tableExists(c, "encounter_types"));
        }
    }

    public void verifyTableDoesNotExist(String table) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            Assert.assertFalse(DatabaseUtil.tableExists(c, "encounter_types"));
        }
    }
}
