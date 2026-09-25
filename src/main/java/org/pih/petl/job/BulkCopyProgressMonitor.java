package org.pih.petl.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.LogUtils;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically logs that a long-running bulk copy is still in progress, with an approximate row count if available.
 * Bulk copies otherwise log nothing until they complete, which makes a slow copy indistinguishable from a hung one.
 */
public class BulkCopyProgressMonitor implements AutoCloseable {

    private static final Log log = LogFactory.getLog(BulkCopyProgressMonitor.class);

    private final String tableName;
    private final Callable<Long> rowCounter;
    private final long startMillis = System.currentTimeMillis();
    private final ScheduledExecutorService executor;
    private volatile boolean rowCountAvailable = true;

    /**
     * @param tableName the table being loaded
     * @param rowCounter returns the approximate number of rows loaded so far, or null if not available
     * @param intervalSeconds how often to log progress
     */
    public BulkCopyProgressMonitor(String tableName, Callable<Long> rowCounter, long intervalSeconds) {
        this.tableName = tableName;
        this.rowCounter = rowCounter;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bulk-copy-progress");
            t.setDaemon(true);
            return t;
        });
        Map<String, String> logContext = MDC.getCopyOfContextMap();
        executor.scheduleAtFixedRate(() -> logProgress(logContext), intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private void logProgress(Map<String, String> logContext) {
        if (logContext != null) {
            MDC.setContextMap(logContext);
        }
        try {
            StringBuilder msg = new StringBuilder("Bulk copy into " + tableName + " still running: ");
            msg.append(LogUtils.formatDuration(System.currentTimeMillis() - startMillis)).append(" elapsed");
            Long rows = countRows();
            if (rows != null) {
                msg.append(String.format(", ~%,d rows loaded", rows));
            }
            log.info(msg);
        }
        catch (Throwable t) {
            log.debug("Unable to log bulk copy progress", t);
        }
        finally {
            MDC.clear();
        }
    }

    private Long countRows() {
        if (rowCountAvailable) {
            try {
                return rowCounter.call();
            }
            catch (Exception e) {
                rowCountAvailable = false;
                log.debug("Row count is not available for bulk copy progress, logging elapsed time only: " + e.getMessage());
            }
        }
        return null;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
