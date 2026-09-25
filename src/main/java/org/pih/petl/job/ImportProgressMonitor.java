package org.pih.petl.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.LogUtils;
import org.pih.petl.PhaseTimer;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically logs that a long-running import is still in progress, with the phase it is in, and during a bulk copy,
 * the approximate number of rows loaded if available.  Imports otherwise log nothing until they complete, which makes
 * a slow import indistinguishable from a hung one.
 */
public class ImportProgressMonitor implements AutoCloseable {

    private static final Log log = LogFactory.getLog(ImportProgressMonitor.class);

    private final PhaseTimer timer;
    private final ScheduledExecutorService executor;
    private volatile String rowCountTable;
    private volatile Callable<Long> rowCounter;
    private volatile boolean rowCountAvailable = true;

    /**
     * @param timer the timer tracking the phases of the import
     * @param intervalSeconds how often to log progress
     */
    public ImportProgressMonitor(PhaseTimer timer, long intervalSeconds) {
        this.timer = timer;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "import-progress");
            t.setDaemon(true);
            return t;
        });
        Map<String, String> logContext = MDC.getCopyOfContextMap();
        executor.scheduleAtFixedRate(() -> logProgress(logContext), intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * @param table the table being loaded, to include in progress messages
     * @param rowCounter returns the approximate number of rows loaded so far, or null to stop reporting row counts
     */
    public void setRowCounter(String table, Callable<Long> rowCounter) {
        this.rowCountTable = table;
        this.rowCounter = rowCounter;
    }

    private void logProgress(Map<String, String> logContext) {
        if (logContext != null) {
            MDC.setContextMap(logContext);
        }
        try {
            log.info(buildProgressMessage());
        }
        catch (Throwable t) {
            log.debug("Unable to log import progress", t);
        }
        finally {
            MDC.clear();
        }
    }

    String buildProgressMessage() {
        StringBuilder msg = new StringBuilder("Still running after " + LogUtils.formatDuration(timer.getTotalMillis()));
        String phase = timer.getCurrentPhase();
        if (phase != null) {
            msg.append(": ").append(phase).append(" for ").append(LogUtils.formatDuration(timer.getCurrentPhaseMillis()));
        }
        Callable<Long> counter = rowCounter;
        String table = rowCountTable;
        if (counter != null) {
            Long rows = countRows(counter);
            if (rows != null) {
                msg.append(String.format(", ~%,d rows loaded into %s", rows, table));
            }
        }
        return msg.toString();
    }

    private Long countRows(Callable<Long> counter) {
        if (rowCountAvailable) {
            try {
                return counter.call();
            }
            catch (Exception e) {
                rowCountAvailable = false;
                log.debug("Row count is not available for import progress, logging elapsed time only: " + e.getMessage());
            }
        }
        return null;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
