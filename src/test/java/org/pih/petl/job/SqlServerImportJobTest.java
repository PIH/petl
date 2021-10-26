package org.pih.petl.job;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
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
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/sqlserverimport"})
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
        etlService.executeJob("job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        etlService.executeJob("job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }

    @Test
    public void testLoadingFromPostgres() throws Exception {
        etlService.executeJob("jobPostgres.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 6);

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        etlService.executeJob("jobPostgres.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 6);
    }

    @Test
    public void testLoadingFromMySQLWithDropAndRecreateTableFalse() throws Exception {
        etlService.executeJob("jobDropAndRecreateTableFalse.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        etlService.executeJob("jobDropAndRecreateTableFalse.yml");
        // since we aren't dropping the table, all rows should be inserts twice, doubling the result set
        // (ignore fact that we really should have a key on uuid, which would result in duplicate key exception)
        verifyRowCount("encounter_types", 124);
    }

    @Test
    public void testLoadingFromMySQLWithExtraColumns() throws Exception {
        etlService.executeJob("jobWithExtraColumns.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
        assertSqlServerCount("select count(*) from encounter_types where import_date is not null", 62);
        assertSqlServerCount("select count(distinct(import_reason)) from encounter_types where import_reason is not null", 1);
    }

    @Test
    public void testConditionalTrue() throws Exception {
        etlService.executeJob("jobConditionalTrue.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }

    @Test
    public void testLoadingWithContext() throws Exception {
        etlService.executeJob("jobWithContext.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 4);
    }

    @Test
    public void testConditionalFalse() throws Exception {
        etlService.executeJob("jobConditionalFalse.yml");
        verifyTableDoesNotExist("encounter_types");
    }

    public void verifyRowCount(String table, int expectedRows) throws Exception {
        assertSqlServerCount("select count(*) from " + table, expectedRows);
    }

    public void verifyTableExists(String table) throws Exception {
        assertSqlServerCount("IF OBJECT_ID ('dbo." + table + "') IS NOT NULL SELECT 1 ELSE SELECT 0", 1);
    }

    public void assertSqlServerCount(String query, Integer expected) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            QueryRunner qr = new QueryRunner();
            Integer result = qr.query(c, query, new ScalarHandler<>());
            Assert.assertEquals(expected, result);
        }
    }

    public void verifyTableDoesNotExist(String table) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            Assert.assertFalse(DatabaseUtil.tableExists(c, table));
        }
    }
}
