package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.pih.petl.PhaseTimer;

public class SqlServerImportJobSummaryTest {

    @Test
    public void shouldSummarizeImportWithRowsPartitionAndPhases() {
        PhaseTimer timer = new PhaseTimer();
        timer.start("setup");
        timer.start("bulk copy");
        timer.stop();
        String summary = SqlServerImportJob.importSummary("obs", "2", 123456, timer);
        Assert.assertTrue(summary, summary.startsWith("Imported 123,456 rows into obs (partition 2) in "));
        Assert.assertTrue(summary, summary.contains("[setup: 0.0s, bulk copy: 0.0s]"));
    }

    @Test
    public void shouldSummarizeImportWithoutRowCountOrPartition() {
        String summary = SqlServerImportJob.importSummary("obs", null, null, new PhaseTimer());
        Assert.assertTrue(summary, summary.startsWith("Imported data into obs in "));
    }
}
