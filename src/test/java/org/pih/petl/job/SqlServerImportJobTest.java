package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.SqlUtils;
import org.pih.petl.api.EtlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/sqlserverimport"})
public class SqlServerImportJobTest extends BasePetlTest {

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
    public void testLoadingFromMySQL() throws Exception {
        executeJob("job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        executeJob("job.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }

    @Test
    public void testLoadingFromPostgres() throws Exception {
        executeJob("jobPostgres.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 6);

        // by default, table should be dropped and recreated on each run, so consecutive runs should return the same result
        executeJob("jobPostgres.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 6);
    }

    @Test
    public void testLoadingFromMySQLWithDropAndRecreateTableFalse() throws Exception {
        executeJob("jobDropAndRecreateTableFalse.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);

        executeJob("jobDropAndRecreateTableFalse.yml");
        // since we aren't dropping the table, all rows should be inserts twice, doubling the result set
        // (ignore fact that we really should have a key on uuid, which would result in duplicate key exception)
        verifyRowCount("encounter_types", 124);
    }

    @Test
    public void testLoadingFromMySQLWithExtraColumns() throws Exception {
        executeJob("jobWithExtraColumns.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
        assertSqlServerCount("select count(*) from encounter_types where import_date is not null", 62);
        assertSqlServerCount("select count(distinct(import_reason)) from encounter_types where import_reason is not null", 1);
    }

    @Test
    public void testLoadingFromMySQLWithPartitions() throws Exception {
        executeJob("jobWithPartitions.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 11);
        assertSqlServerCount("select count(*) from encounter_types where partition_num = 1", 7);
        assertSqlServerCount("select count(*) from encounter_types where partition_num = 2", 4);
    }

    @Test
    public void testLoadingFromMySQLWithPartitionsAndSchemaChange() throws Exception {
        executeJob("jobWithPartitionsAndSchemaChange.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 4);
        assertSqlServerCount("select count(*) from encounter_types where partition_num = 1", 0);
        assertSqlServerCount("select count(*) from encounter_types where partition_num = 2", 4);
    }

    @Test
    public void testConditionalTrue() throws Exception {
        executeJob("jobConditionalTrue.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 62);
    }

    @Test
    public void testLoadingWithContext() throws Exception {
        executeJob("jobWithContext.yml");
        verifyTableExists("encounter_types");
        verifyRowCount("encounter_types", 4);
    }

    @Test
    public void testConditionalFalse() throws Exception {
        executeJob("jobConditionalFalse.yml");
        verifyTableDoesNotExist("encounter_types");
    }

    @Test
    public void testInaccessibleSource() throws Exception {
        Exception foundException = null;
        try {
            executeJob("jobWithInaccessibleSource.yml");
            verifyTableDoesNotExist("encounter_types");
        }
        catch (Exception e) {
            foundException = e;
        }
        Assert.assertNotNull(foundException);
    }

    @Test
    public void testInaccessibleTarget() throws Exception {
        Exception foundException = null;
        try {
            executeJob("jobWithInaccessibleTarget.yml");
            verifyTableDoesNotExist("encounter_types");
        }
        catch (Exception e) {
            foundException = e;
        }
        Assert.assertNotNull(foundException);
    }

    @Test
    public void shouldSupportIncrementalLoading() throws Exception {
        // Initial state
        Date date1 = queryMysql("select last_updated from encounter_type_changes where uuid = 'aa61d509-6e76-4036-a65d-7813c0c3b752'");
        Assertions.assertEquals("2022-02-04T10:32:15.012Z", SqlUtils.isoDate(date1));
        Date date2 = queryMysql("select last_updated from encounter_type_changes where uuid = '55a0d3ea-a4d7-4e88-8f01-5aceb2d3c61b'");
        Assertions.assertEquals("2022-02-04T22:11:19.556Z", SqlUtils.isoDate(date2));
        Date date3 = queryMysql("select last_updated from encounter_type_changes where uuid = '1e2a509c-7c9f-11e9-8f9e-2a86e4085a59'");
        Assertions.assertEquals("2022-02-05T09:54:09.112Z", SqlUtils.isoDate(date3));

        executeJob("jobWithPartitionsIncremental.yml");
        verifyTableExists("encounter_type_changes");
        verifyRowCount("encounter_type_changes", 3);
        Date endingWatermark = getSqlServerDatasource().querySingleValue("select ending_watermark from petl_incremental_update_log where table_name = 'encounter_types'");
        Assertions.assertEquals("2022-02-05T09:54:09.112Z", SqlUtils.isoDate(endingWatermark));
    }
}
