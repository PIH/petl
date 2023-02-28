package org.pih.petl.job;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecution;
import org.pih.petl.api.JobExecutor;
import org.pih.petl.job.config.DataSource;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.util.List;

public abstract class BasePetlTest {

    @Autowired
    EtlService etlService;

    public JobExecution executeJob(String jobKey) {
        JobExecutor executor = new JobExecutor(etlService, 1);
        return executor.executeJob(jobKey);
    }

    public Exception executeJobAndReturnException(String jobKey) {
        Exception exception = null;
        try {
            executeJob(jobKey);
        }
        catch (Exception e) {
            exception = e;
        }
        return exception;
    }

    abstract List<String> getTablesCreated();

    @Before
    public void runBefore() throws Exception {
        dropTablesInTargetDB();
    }

    @After
    public void runAfter() throws Exception {
        dropTablesInTargetDB();
    }

    public DataSource getSqlServerDatasource() {
        return etlService.getApplicationConfig().getEtlDataSource("sqlserver-testcontainer.yml");
    }

    public void dropTablesInTargetDB() throws Exception{
        for (String tableName : getTablesCreated()) {
            getSqlServerDatasource().dropTableIfExists(tableName);
        }
    }

    public void verifyNoTablesExist() throws Exception {
        for (String tableName : getTablesCreated()) {
            verifyTableDoesNotExist(tableName);
        }
    }

    public void verifyAllTablesExist() throws Exception {
        for (String tableName : getTablesCreated()) {
            verifyTableExists(tableName);
        }
    }

    public void verifyRowCount(String table, int expectedRows) throws Exception {
        Assert.assertEquals(expectedRows, getSqlServerDatasource().rowCount(table));
    }

    public void verifyTableExists(String table) throws Exception {
        Assert.assertTrue(getSqlServerDatasource().tableExists(table));
    }

    public void verifyTableDoesNotExist(String table) throws Exception {
        Assert.assertFalse(getSqlServerDatasource().tableExists(table));
    }

    public void assertSqlServerCount(String query, Integer expected) throws Exception {
        ApplicationConfig appConfig = etlService.getApplicationConfig();
        DataSource sqlServerDataSource = appConfig.getEtlDataSource("sqlserver-testcontainer.yml");
        try (Connection c = sqlServerDataSource.openConnection()) {
            QueryRunner qr = new QueryRunner();
            Integer result = qr.query(c, query, new ScalarHandler<>());
            Assert.assertEquals(expected, result);
        }
    }
}
