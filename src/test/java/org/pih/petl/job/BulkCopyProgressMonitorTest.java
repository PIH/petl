package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class BulkCopyProgressMonitorTest {

    @Test
    public void shouldPeriodicallyCountRowsUntilClosed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (BulkCopyProgressMonitor ignored = new BulkCopyProgressMonitor("my_table", () -> (long) calls.incrementAndGet(), 1)) {
            Thread.sleep(2500);
        }
        int callsWhenClosed = calls.get();
        Assert.assertEquals(2, callsWhenClosed);
        Thread.sleep(1200);
        Assert.assertEquals(callsWhenClosed, calls.get());
    }

    @Test
    public void shouldStopCountingRowsIfCountFails() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (BulkCopyProgressMonitor ignored = new BulkCopyProgressMonitor("my_table", () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("VIEW DATABASE STATE permission denied");
        }, 1)) {
            Thread.sleep(2500);
        }
        Assert.assertEquals(1, calls.get());
    }
}
