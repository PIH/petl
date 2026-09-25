package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Writes real-time run monitoring files to $PETL_HOME/logs/:
 *   petl-history.log - one line per job start and completion, suitable for tail -f
 *   petl-status.log - current run state, rewritten periodically while a run is active
 *
 * Run state is tracked in memory from job execution saves, so building the status file does not query
 * petl_job_execution while jobs are concurrently writing to it.  The database is only read when a run is first
 * seen, to pick up child executions from a previous attempt of a resumed run.
 *
 * Monitoring is best-effort: no public method throws, so a monitoring failure can never fail a job.
 */
@Component
public class RunMonitor {

    private static final Log log = LogFactory.getLog(RunMonitor.class);

    private static final String RUN_LOG    = "petl-history.log";
    private static final String STATUS_LOG = "petl-status.log";
    private static final String LINE = "================================================================================";

    private static final long FLUSH_INTERVAL_SECONDS = 5;
    private static final long REFRESH_INTERVAL_MILLIS = 30000;

    private final ScheduledExecutorService refreshTimer = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, JobExecution> executions = new ConcurrentHashMap<>();
    private final Set<String> loadedRoots = ConcurrentHashMap.newKeySet();

    private volatile String activeRootUuid;
    private volatile boolean dirty;
    private volatile long lastWriteMillis;

    @Autowired
    private ApplicationConfig applicationConfig;

    @PostConstruct
    public void init() {
        refreshTimer.scheduleWithFixedDelay(() -> flushStatusFile(false), FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        refreshTimer.shutdownNow();
    }

    /**
     * Called whenever a JobExecution is saved, to keep the in-memory run state current
     */
    public void onSave(JobExecution execution) {
        try {
            executions.put(execution.getUuid(), execution);
            dirty = true;
        }
        catch (Throwable t) {
            log.warn("Run monitor failed to record job execution save: " + t.getMessage());
        }
    }

    public void onJobStart(JobExecution execution, EtlService etlService) {
        try {
            if (execution.getParentExecutionUuid() == null) {
                appendRunSeparator(execution);
            }
            appendToRunLog(execution, "-");
            activate(execution, etlService);
        }
        catch (Throwable t) {
            log.warn("Run monitor failed on job start: " + t.getMessage());
        }
    }

    public void onJobComplete(JobExecution execution, EtlService etlService) {
        try {
            appendToRunLog(execution, RunSummaryLogger.formatDuration(execution));
            activate(execution, etlService);
            if (execution.getParentExecutionUuid() == null) {
                flushStatusFile(true);
            }
        }
        catch (Throwable t) {
            log.warn("Run monitor failed on job complete: " + t.getMessage());
        }
    }

    /**
     * Makes the run containing this execution the one reported in the status file
     */
    private void activate(JobExecution execution, EtlService etlService) {
        executions.putIfAbsent(execution.getUuid(), execution);
        JobExecution root = findRoot(execution, etlService);
        if (loadedRoots.add(root.getUuid())) {
            loadExistingExecutions(root, etlService);
        }
        activeRootUuid = root.getUuid();
        dirty = true;
    }

    private JobExecution findRoot(JobExecution execution, EtlService etlService) {
        JobExecution root = execution;
        while (root.getParentExecutionUuid() != null) {
            JobExecution parent = executions.get(root.getParentExecutionUuid());
            if (parent == null) {
                parent = etlService.getJobExecution(root.getParentExecutionUuid());
                if (parent == null) {
                    break;
                }
                executions.putIfAbsent(parent.getUuid(), parent);
            }
            root = parent;
        }
        return root;
    }

    /**
     * Loads child executions that already exist in the database, eg. from a previous attempt of a resumed run.
     * Existing in-memory instances are kept, as they reflect the most recent saves.
     */
    private void loadExistingExecutions(JobExecution execution, EtlService etlService) {
        for (JobExecution child : etlService.getChildExecutions(execution)) {
            executions.putIfAbsent(child.getUuid(), child);
            loadExistingExecutions(child, etlService);
        }
    }

    /**
     * Rewrites the status file if anything has changed, or if it has not been refreshed recently
     * When the active run has completed, the final status is written and its state is released.
     */
    synchronized void flushStatusFile(boolean force) {
        try {
            String rootUuid = activeRootUuid;
            JobExecution root = (rootUuid == null ? null : executions.get(rootUuid));
            if (root == null) {
                return;
            }
            long now = System.currentTimeMillis();
            if (!force && !dirty && now - lastWriteMillis < REFRESH_INTERVAL_MILLIS) {
                return;
            }
            dirty = false;
            lastWriteMillis = now;

            Map<String, List<JobExecution>> childIndex = buildChildIndex();
            writeStatusFile(buildStatusContent(root, childIndex));

            if (isTerminal(root.getStatus())) {
                release(root, childIndex);
            }
        }
        catch (Throwable t) {
            log.warn("Run monitor failed to update " + STATUS_LOG + ": " + t.getMessage());
        }
    }

    private void release(JobExecution root, Map<String, List<JobExecution>> childIndex) {
        if (root.getUuid().equals(activeRootUuid)) {
            activeRootUuid = null;
        }
        List<JobExecution> descendants = new ArrayList<>();
        collectDescendants(root, childIndex, descendants);
        for (JobExecution d : descendants) {
            executions.remove(d.getUuid());
        }
        executions.remove(root.getUuid());
        loadedRoots.remove(root.getUuid());
    }

    private Map<String, List<JobExecution>> buildChildIndex() {
        Map<String, List<JobExecution>> childIndex = new HashMap<>();
        for (JobExecution e : executions.values()) {
            if (e.getParentExecutionUuid() != null) {
                childIndex.computeIfAbsent(e.getParentExecutionUuid(), k -> new ArrayList<>()).add(e);
            }
        }
        Comparator<JobExecution> bySequence = Comparator.comparing(
                JobExecution::getSequenceNum, Comparator.nullsLast(Comparator.naturalOrder())
        );
        for (List<JobExecution> children : childIndex.values()) {
            children.sort(bySequence);
        }
        return childIndex;
    }

    private List<JobExecution> getChildren(JobExecution execution, Map<String, List<JobExecution>> childIndex) {
        return childIndex.getOrDefault(execution.getUuid(), Collections.emptyList());
    }

    private void appendRunSeparator(JobExecution execution) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String line = "--- " + timestamp + "  " + RunSummaryLogger.label(execution) + " ---" + System.lineSeparator();
        try {
            Files.write(logFile(RUN_LOG).toPath(), line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            log.warn("Could not write to " + RUN_LOG + ": " + e.getMessage());
        }
    }

    private void appendToRunLog(JobExecution execution, String duration) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String status    = String.format("%-16s", execution.getStatus().toString());
        String durCol    = String.format("%11s", duration);
        String label     = RunSummaryLogger.label(execution);
        String line      = timestamp + "  " + status + "  " + durCol + "  " + label + System.lineSeparator();
        try {
            File f = logFile(RUN_LOG);
            Files.write(f.toPath(), line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            log.warn("Could not write to " + RUN_LOG + ": " + e.getMessage());
        }
    }

    private void writeStatusFile(String content) {
        try {
            Files.write(logFile(STATUS_LOG).toPath(), content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
        catch (IOException e) {
            log.warn("Could not write to " + STATUS_LOG + ": " + e.getMessage());
        }
    }

    private String buildStatusContent(JobExecution root, Map<String, List<JobExecution>> childIndex) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder sb = new StringBuilder();
        sb.append(LINE).append("\n");
        sb.append("PETL Run Status  (as of ").append(sdf.format(new Date())).append(")\n");
        sb.append(LINE).append("\n");
        sb.append(String.format("  %-12s%s%n", "Job:",     RunSummaryLogger.label(root)));
        sb.append(String.format("  %-12s%s%n", "Status:",  root.getStatus().toString()));
        if (root.getStarted() != null) {
            sb.append(String.format("  %-12s%s%n", "Started:", sdf.format(root.getStarted())));
        }
        if (root.getCompleted() != null) {
            sb.append(String.format("  %-12s%s%n", "Completed:", sdf.format(root.getCompleted())));
        }
        sb.append(String.format("  %-12s%s%n", "Duration:", RunSummaryLogger.formatDuration(root)));

        List<JobExecution> descendants = new ArrayList<>();
        collectDescendants(root, childIndex, descendants);

        if (!descendants.isEmpty()) {
            int total = descendants.size();
            int complete = 0;
            for (JobExecution d : descendants) {
                if (isTerminal(d.getStatus())) { complete++; }
            }
            sb.append(String.format("  %-12s%d / %d complete%n", "Progress:", complete, total));

            sb.append("\n  All jobs:\n");
            appendTree(sb, getChildren(root, childIndex), childIndex, 0);
        }

        sb.append(LINE).append("\n");
        return sb.toString();
    }

    private void collectDescendants(JobExecution execution, Map<String, List<JobExecution>> childIndex, List<JobExecution> result) {
        for (JobExecution child : getChildren(execution, childIndex)) {
            result.add(child);
            collectDescendants(child, childIndex, result);
        }
    }

    private boolean isTerminal(JobExecutionStatus status) {
        return status == JobExecutionStatus.SUCCEEDED
                || status == JobExecutionStatus.FAILED
                || status == JobExecutionStatus.ABORTED;
    }

    private void appendTree(StringBuilder sb, List<JobExecution> executions, Map<String, List<JobExecution>> childIndex, int depth) {
        String indent = buildIndent(depth);
        for (JobExecution exec : executions) {
            String status   = String.format("%-11s", exec.getStatus().toString());
            String duration = String.format("%10s", RunSummaryLogger.formatDuration(exec));
            sb.append(indent).append(status).append("  ").append(duration)
              .append("  ").append(RunSummaryLogger.label(exec)).append("\n");
            List<JobExecution> children = getChildren(exec, childIndex);
            if (!children.isEmpty()) {
                appendTree(sb, children, childIndex, depth + 1);
            }
        }
    }

    private File logFile(String filename) throws IOException {
        File logsDir = new File(applicationConfig.getPetlHomeDir(), "logs");
        if (!logsDir.exists() && !logsDir.mkdirs()) {
            throw new IOException("Could not create logs directory: " + logsDir);
        }
        return new File(logsDir, filename);
    }

    private String buildIndent(int depth) {
        StringBuilder sb = new StringBuilder("  "); // base indent
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }
}
