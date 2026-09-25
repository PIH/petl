package org.pih.petl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.MDC;

import java.sql.SQLException;

public class LogUtilsTest {

    @After
    public void teardown() {
        MDC.clear();
    }

    @Test
    public void shouldFormatDurations() {
        Assert.assertEquals("0.3s", LogUtils.formatDuration(300));
        Assert.assertEquals("9.9s", LogUtils.formatDuration(9900));
        Assert.assertEquals("42s", LogUtils.formatDuration(42000));
        Assert.assertEquals("14m 03s", LogUtils.formatDuration((14 * 60 + 3) * 1000));
        Assert.assertEquals("2h 05m 00s", LogUtils.formatDuration((2 * 3600 + 5 * 60) * 1000));
    }

    @Test
    public void shouldSummarizeFailureAndRootCauseWithSqlErrorCode() {
        SQLException deadlock = new SQLException("Transaction was deadlocked", "40001", 1205);
        Exception e = new JobFailedException("1 of 3 jobs failed", new PetlException("Error in statement 2 of 3 in x.sql", deadlock));
        Assert.assertEquals(
                "1 of 3 jobs failed: PetlException: Error in statement 2 of 3 in x.sql - caused by SQLException: Transaction was deadlocked [SQL error code: 1205, SQLState: 40001]",
                LogUtils.summarizeException(e)
        );
    }

    @Test
    public void shouldSummarizeJobFailedExceptionWithoutCause() {
        Assert.assertEquals("JobFailedException: 2 of 3 jobs failed", LogUtils.summarizeException(new JobFailedException("2 of 3 jobs failed")));
    }

    @Test
    public void shouldSummarizeExceptionWithoutCause() {
        Assert.assertEquals("PetlException: Simulated", LogUtils.summarizeException(new PetlException("Simulated")));
        Assert.assertEquals("", LogUtils.summarizeException(null));
    }

    @Test
    public void shouldAbbreviateSql() {
        String sql = "-- A comment\ninsert into   my_table\n    (id, name)\n  values (1, 'a')";
        Assert.assertEquals("insert into my_table (id, name) values (1, 'a')", LogUtils.abbreviateSql(sql, 200));
        Assert.assertEquals("insert into...", LogUtils.abbreviateSql(sql, 14));
    }

    @Test
    public void shouldDescribeHeapUsage() {
        LogUtils.resetPeakHeapUsage();
        Assert.assertTrue(LogUtils.describeHeapUsage().matches("[0-9,]+ MB used, [0-9,]+ MB peak, [0-9,]+ MB max"));
    }

    @Test
    public void shouldSetAndRestoreJobContext() {
        Assert.assertNull(LogUtils.setJobContext("parent"));
        String previous = LogUtils.setJobContext("child");
        Assert.assertEquals("parent", previous);
        Assert.assertEquals("child", MDC.get(LogUtils.JOB_CONTEXT_KEY));
        LogUtils.setJobContext(previous);
        Assert.assertEquals("parent", MDC.get(LogUtils.JOB_CONTEXT_KEY));
        LogUtils.setJobContext(null);
        Assert.assertNull(MDC.get(LogUtils.JOB_CONTEXT_KEY));
    }
}
