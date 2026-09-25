package org.pih.petl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.MDC;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.sql.SQLException;

/**
 * Helpers for producing consistent, informative log output
 */
public class LogUtils {

    /**
     * MDC key holding the job that the current thread is executing, included in each log line by the log pattern
     */
    public static final String JOB_CONTEXT_KEY = "petlJob";

    /**
     * Sets the job context for the current thread
     * @param jobContext the job context to set, or null to clear it
     * @return the previous job context, so that it can be restored
     */
    public static String setJobContext(String jobContext) {
        String previous = MDC.get(JOB_CONTEXT_KEY);
        if (jobContext == null) {
            MDC.remove(JOB_CONTEXT_KEY);
        }
        else {
            MDC.put(JOB_CONTEXT_KEY, jobContext);
        }
        return previous;
    }

    /**
     * @return a human-readable duration, eg. "0.3s", "42s", "14m 03s", "2h 05m 00s"
     */
    public static String formatDuration(long millis) {
        if (millis < 10000) {
            return String.format("%.1fs", millis / 1000.0);
        }
        long total = millis / 1000;
        long h = total / 3600;
        long m = (total % 3600) / 60;
        long s = total % 60;
        if (h > 0) { return String.format("%dh %02dm %02ds", h, m, s); }
        if (m > 0) { return String.format("%dm %02ds", m, s); }
        return String.format("%ds", s);
    }

    /**
     * @return a one-line summary of the given exception: the message of the first exception in the cause chain that
     * describes the failure (skipping JobFailedException wrappers), followed by the root cause if different.  If the
     * exception itself is a JobFailedException (eg. "2 of 85 jobs failed"), its message is included as a prefix.  If a
     * SQLException is in the cause chain, the vendor error code and SQLState of the innermost one are included, as
     * these identify the type of failure (eg. deadlock, lock timeout, connection failure)
     */
    public static String summarizeException(Throwable t) {
        if (t == null) {
            return "";
        }
        Throwable[] chain = ExceptionUtils.getThrowables(t);
        Throwable first = t;
        for (Throwable cause : chain) {
            if (!(cause instanceof JobFailedException)) {
                first = cause;
                break;
            }
        }
        Throwable rootCause = chain[chain.length - 1];
        StringBuilder sb = new StringBuilder();
        if (t instanceof JobFailedException && first != t) {
            sb.append(t.getMessage()).append(": ");
        }
        sb.append(ExceptionUtils.getMessage(first));
        if (rootCause != first) {
            sb.append(" - caused by ").append(ExceptionUtils.getMessage(rootCause));
        }
        SQLException sqlException = null;
        for (Throwable cause : chain) {
            if (cause instanceof SQLException) {
                sqlException = (SQLException) cause;
            }
        }
        if (sqlException != null) {
            sb.append(" [SQL error code: ").append(sqlException.getErrorCode());
            sb.append(", SQLState: ").append(sqlException.getSQLState()).append("]");
        }
        return sb.toString();
    }

    /**
     * Resets the peak heap usage, so that it reflects usage from this point, eg. the start of a run
     */
    public static void resetPeakHeapUsage() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                pool.resetPeakUsage();
            }
        }
    }

    /**
     * @return current, peak, and maximum heap usage, eg. "1,234 MB used, 3,456 MB peak, 4,096 MB max".  The peak is
     * approximate, as it sums the peak of each heap memory pool, which may not have occurred at the same time
     */
    public static String describeHeapUsage() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long peak = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null) {
                peak += pool.getPeakUsage().getUsed();
            }
        }
        long mb = 1024 * 1024;
        return String.format("%,d MB used, %,d MB peak, %,d MB max", heap.getUsed() / mb, peak / mb, heap.getMax() / mb);
    }

    /**
     * @return the given sql with comments removed and whitespace collapsed, abbreviated to the given length
     */
    public static String abbreviateSql(String sql, int maxLength) {
        if (sql == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String line : sql.split("\\r?\\n")) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("--") && !trimmed.startsWith("#")) {
                sb.append(trimmed).append(" ");
            }
        }
        String collapsed = sb.toString().replaceAll("\\s+", " ").trim();
        return StringUtils.abbreviate(collapsed, maxLength);
    }
}
