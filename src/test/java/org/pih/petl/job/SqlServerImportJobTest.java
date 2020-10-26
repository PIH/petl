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
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SqlServerImportJobTest {

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
        etlService.executeJob("sqlserverimport/job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        etlService.executeJob("sqlserverimport/job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }


    @Test
    public void testLoadingFromMySQLWithDropAndRecreateTableFalse() throws Exception {
        etlService.executeJob("sqlserverimport/jobDropAndRecreateTableFalse.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        etlService.executeJob("sqlserverimport/jobDropAndRecreateTableFalse.yml");
        // since we aren't dropping the table, all rows should be inserts twice, doubling the result set
        // (ignore fact that we really should have a key on uuid, which would result in duplicate key exception)
        verifyRowCount("encounter_types", 124);
    }

    @Test
    public void testConditionalTrue() throws Exception {
        etlService.executeJob("sqlserverimport/jobConditionalTrue.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }

    @Test
    public void testConditionalFalse() throws Exception {
        etlService.executeJob("sqlserverimport/jobConditionalFalse.yml");
        verifyTableDoesNotExist("encounter_types");
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
