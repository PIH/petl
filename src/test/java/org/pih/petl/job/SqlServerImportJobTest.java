package org.pih.petl.job;

import java.sql.Connection;

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

    @Test
    public void testLoadingFromMySQL() throws Exception {
        etlService.executeJob("sqlserverimport/job.yml");
        verifyRowCount("encounter_types", 62);
    }

    public void verifyRowCount(String table, int expectedRows) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        EtlDataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = DatabaseUtil.openConnection(sqlServerDataSource)) {
            int rowsFound = DatabaseUtil.rowCount(c, table);
            Assert.assertEquals(expectedRows, rowsFound);
        }
    }
}
