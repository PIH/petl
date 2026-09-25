package org.pih.petl.job;

import org.junit.Assert;
import org.junit.Test;
import org.pih.petl.PhaseTimer;

import java.util.concurrent.atomic.AtomicInteger;

public class ImportProgressMonitorTest {

    @Test
    public void shouldReportCurrentPhase() {
        PhaseTimer timer = new PhaseTimer();
        timer.start("extract prep");
        try (ImportProgressMonitor monitor = new ImportProgressMonitor(timer, 3600)) {
            String message = monitor.buildProgressMessage();
            Assert.assertTrue(message, message.matches("Still running after [0-9.]+s: extract prep for [0-9.]+s"));
        }
    }

    @Test
    public void shouldReportRowsOnlyWhileRowCounterIsSet() {
        PhaseTimer timer = new PhaseTimer();
        timer.start("bulk copy");
        try (ImportProgressMonitor monitor = new ImportProgressMonitor(timer, 3600)) {
            monitor.setRowCounter("obs_2", () -> 97650L);
            Assert.assertTrue(monitor.buildProgressMessage().endsWith(": bulk copy for 0.0s, ~97,650 rows loaded into obs_2"));
            monitor.setRowCounter(null, null);
            Assert.assertFalse(monitor.buildProgressMessage().contains("rows loaded"));
        }
    }

    @Test
    public void shouldStopCountingRowsIfCountFails() {
        AtomicInteger calls = new AtomicInteger();
        PhaseTimer timer = new PhaseTimer();
        timer.start("bulk copy");
        try (ImportProgressMonitor monitor = new ImportProgressMonitor(timer, 3600)) {
            monitor.setRowCounter("obs_2", () -> {
                calls.incrementAndGet();
                throw new IllegalStateException("VIEW DATABASE STATE permission denied");
            });
            monitor.buildProgressMessage();
            String message = monitor.buildProgressMessage();
            Assert.assertEquals(1, calls.get());
            Assert.assertTrue(message, message.endsWith("bulk copy for 0.0s"));
        }
    }

    @Test
    public void shouldLogPeriodicallyUntilClosed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PhaseTimer timer = new PhaseTimer();
        timer.start("bulk copy");
        try (ImportProgressMonitor monitor = new ImportProgressMonitor(timer, 1)) {
            monitor.setRowCounter("obs_2", () -> (long) calls.incrementAndGet());
            Thread.sleep(2500);
        }
        int callsWhenClosed = calls.get();
        Assert.assertEquals(2, callsWhenClosed);
        Thread.sleep(1200);
        Assert.assertEquals(callsWhenClosed, calls.get());
    }
}
