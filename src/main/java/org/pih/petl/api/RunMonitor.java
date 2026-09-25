package org.pih.petl.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.LogUtils;
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
import java.util.LinkedHashMap;
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
    private static final long PROGRESS_LOG_INTERVAL_MILLIS = 5 * 60 * 1000;
    private static final int MAX_RELEASED_ROOTS = 100;

    private final ScheduledExecutorService refreshTimer = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, JobExecution> executions = new ConcurrentHashMap<>();
    private final Set<String> loadedRoots = ConcurrentHashMap.newKeySet();

    // Recently completed runs, so that repeat notifications for them do not reload them from the database
    private final Set<String> releasedRoots = Collections.synchronizedSet(Collections.newSetFromMap(
            new LinkedHashMap<String, Boolean>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_RELEASED_ROOTS;
                }
            }
    ));

    private volatile String activeRootUuid;
    private volatile boolean dirty;
    private volatile long lastWriteMillis;
    private String progressRootUuid;
    private long lastProgressLogMillis;

    @Autowired
    private ApplicationConfig applicationConfig;

    @PostConstruct
    public void init() {
        refreshTimer.scheduleWithFixedDelay(() -> {
            String rootUuid = activeRootUuid;
            logProgressIfDue(rootUuid);
            flushStatusFile(rootUuid, false);
        }, FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS);
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
            if (releasedRoots.contains(execution.getUuid())) {
                if (isTerminal(execution.getStatus())) {
                    return;
                }
                releasedRoots.remove(execution.getUuid()); // A completed run is being resumed
            }
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
            String rootUuid = activate(execution, etlService);
            if (rootUuid != null && execution.getParentExecutionUuid() == null) {
                // Flush this specific run, as a concurrent run may have since become active
                flushStatusFile(rootUuid, true);
            }
        }
        catch (Throwable t) {
            log.warn("Run monitor failed on job complete: " + t.getMessage());
        }
    }

    /**
     * Makes the run containing this execution the one reported in the status file
     * @return the uuid of the root execution of the run, or null if the run has already completed and been released
     */
    private String activate(JobExecution execution, EtlService etlService) {
        if (releasedRoots.contains(execution.getUuid()) && isTerminal(execution.getStatus())) {
            return null;
        }
        executions.putIfAbsent(execution.getUuid(), execution);
        JobExecution root = findRoot(execution, etlService);
        if (loadedRoots.add(root.getUuid())) {
            loadExistingExecutions(root, etlService);
        }
        activeRootUuid = root.getUuid();
        dirty = true;
        return root.getUuid();
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
     * Rewrites the status file for the given run if anything has changed, or if it has not been refreshed recently.
     * When the run has completed, the final status is written and its state is released.  Any other runs that have
     * completed without being flushed (eg. resumed runs, which do not notify on completion) are also released.
     */
    synchronized void flushStatusFile(String rootUuid, boolean force) {
        try {
            JobExecution root = (rootUuid == null ? null : executions.get(rootUuid));
            long now = System.currentTimeMillis();
            if (root != null && (force || dirty || now - lastWriteMillis >= REFRESH_INTERVAL_MILLIS)) {
                dirty = false;
                lastWriteMillis = now;
                Map<String, List<JobExecution>> childIndex = buildChildIndex();
                writeStatusFile(buildStatusContent(root, childIndex));
                if (isTerminal(root.getStatus())) {
                    release(root, childIndex);
                }
            }
            releaseOtherCompletedRuns(rootUuid);
        }
        catch (Throwable t) {
            log.warn("Run monitor failed to update " + STATUS_LOG + ": " + t.getMessage());
        }
    }

    /**
     * Periodically logs the progress of the active run, so that it can be followed in the main log
     */
    synchronized void logProgressIfDue(String rootUuid) {
        try {
            JobExecution root = (rootUuid == null ? null : executions.get(rootUuid));
            if (root == null || isTerminal(root.getStatus())) {
                return;
            }
            long now = System.currentTimeMillis();
            if (!rootUuid.equals(progressRootUuid)) {
                progressRootUuid = rootUuid;
                lastProgressLogMillis = now;
            }
            else if (now - lastProgressLogMillis >= PROGRESS_LOG_INTERVAL_MILLIS) {
                lastProgressLogMillis = now;
                String previousJobContext = LogUtils.setJobContext(RunSummaryLogger.label(root));
                try {
                    log.info(buildProgressMessage(root, buildChildIndex()));
                }
                finally {
                    LogUtils.setJobContext(previousJobContext);
                }
            }
        }
        catch (Throwable t) {
            log.warn("Run monitor failed to log progress: " + t.getMessage());
        }
    }

    /**
     * @return the progress of the run, counting leaf jobs (those that do the work, rather than group other jobs).
     * The total grows as parent jobs start and create their child jobs.
     */
    String buildProgressMessage(JobExecution root, Map<String, List<JobExecution>> childIndex) {
        List<JobExecution> descendants = new ArrayList<>();
        collectDescendants(root, childIndex, descendants);
        int total = 0, succeeded = 0, failed = 0, inProgress = 0;
        for (JobExecution d : descendants) {
            if (getChildren(d, childIndex).isEmpty()) {
                total++;
                JobExecutionStatus status = d.getStatus();
                if (status == JobExecutionStatus.SUCCEEDED) {
                    succeeded++;
                }
                else if (status == JobExecutionStatus.FAILED || status == JobExecutionStatus.ABORTED) {
                    failed++;
                }
                else if (status == JobExecutionStatus.IN_PROGRESS) {
                    inProgress++;
                }
            }
        }
        return String.format("Run progress: %,d of %,d jobs complete (%,d succeeded, %,d failed), %,d in progress, after %s",
                succeeded + failed, total, succeeded, failed, inProgress, RunSummaryLogger.formatDuration(root));
    }

    private void releaseOtherCompletedRuns(String excludedRootUuid) {
        Map<String, List<JobExecution>> childIndex = null;
        for (String uuid : new ArrayList<>(loadedRoots)) {
            JobExecution root = executions.get(uuid);
            if (!uuid.equals(excludedRootUuid) && root != null && isTerminal(root.getStatus())) {
                if (childIndex == null) {
                    childIndex = buildChildIndex();
                }
                release(root, childIndex);
            }
        }
    }

    /**
     * Removes the given run from memory.  If it was the active run, another in-progress run becomes active.
     */
    private void release(JobExecution root, Map<String, List<JobExecution>> childIndex) {
        List<JobExecution> descendants = new ArrayList<>();
        collectDescendants(root, childIndex, descendants);
        for (JobExecution d : descendants) {
            executions.remove(d.getUuid());
        }
        executions.remove(root.getUuid());
        loadedRoots.remove(root.getUuid());
        releasedRoots.add(root.getUuid());
        if (activeRootUuid == null || root.getUuid().equals(activeRootUuid)) {
            activeRootUuid = null;
            for (String uuid : loadedRoots) {
                JobExecution other = executions.get(uuid);
                if (other != null && !isTerminal(other.getStatus())) {
                    activeRootUuid = uuid;
                    dirty = true;
                    break;
                }
            }
        }
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
