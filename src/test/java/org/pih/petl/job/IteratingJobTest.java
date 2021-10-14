package org.pih.petl.job;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.job.datasource.DatabaseUtil;
import org.pih.petl.job.datasource.EtlDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class IteratingJobTest {

    @Autowired
    EtlService etlService;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Before
    public void runBefore() throws Exception {
        dropTablesInTargetDB();
    }

    @After
    public void runAfter() throws Exception {
        dropTablesInTargetDB();
    }

    public void dropTablesInTargetDB() throws Exception{
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            DatabaseUtil.dropTable(c, "encounter_types");
        }
    }

    @Test
    public void testJobWithSqlExecution() throws Exception {
        etlService.executeJob("iteratingjob/jobWithSqlExecution.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 13);
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
