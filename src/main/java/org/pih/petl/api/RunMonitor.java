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
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes real-time run monitoring files to $PETL_HOME/logs/:
 *   petl-run.log - one line per job completion, suitable for tail -f
 *   petl-status.log - current run state, rewritten on each completion
 */
@Component
public class RunMonitor {

    private static final Log log = LogFactory.getLog(RunMonitor.class);

    private static final String RUN_LOG    = "petl-history.log";
    private static final String STATUS_LOG = "petl-status.log";
    private static final String LINE = "================================================================================";

    private final ReentrantLock statusLock = new ReentrantLock();
    private final ScheduledExecutorService refreshTimer = Executors.newSingleThreadScheduledExecutor();

    private volatile JobExecution activeRoot;
    private volatile EtlService activeEtlService;

    @Autowired
    private ApplicationConfig applicationConfig;

    @PostConstruct
    public void init() {
        refreshTimer.scheduleAtFixedRate(this::refreshStatusFile, 30, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        refreshTimer.shutdownNow();
    }

    private void refreshStatusFile() {
        JobExecution root = activeRoot;
        EtlService etlService = activeEtlService;
        if (root != null && etlService != null) {
            rewriteStatusFile(root, etlService);
        }
    }

    public void onJobStart(JobExecution execution, EtlService etlService) {
        if (execution.getParentExecutionUuid() == null) {
            activeRoot = execution;
            activeEtlService = etlService;
            appendRunSeparator(execution);
        }
        appendToRunLog(execution, "-");
        rewriteStatusFile(execution, etlService);
    }

    public void onJobComplete(JobExecution execution, EtlService etlService) {
        appendToRunLog(execution, RunSummaryLogger.formatDuration(execution));
        if (execution.getParentExecutionUuid() == null) {
            activeRoot = null;
            activeEtlService = null;
        }
        rewriteStatusFile(execution, etlService);
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

    private void rewriteStatusFile(JobExecution execution, EtlService etlService) {
        JobExecution root = findRoot(execution, etlService);
        String content = buildStatusContent(root, etlService);
        statusLock.lock();
        try {
            Files.write(logFile(STATUS_LOG).toPath(), content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
        catch (IOException e) {
            log.warn("Could not write to " + STATUS_LOG + ": " + e.getMessage());
        }
        finally {
            statusLock.unlock();
        }
    }

    private String buildStatusContent(JobExecution root, EtlService etlService) {
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
        collectDescendants(root, etlService, descendants);

        if (!descendants.isEmpty()) {
            int total = descendants.size();
            int complete = 0;
            for (JobExecution d : descendants) {
                if (isTerminal(d.getStatus())) { complete++; }
            }
            sb.append(String.format("  %-12s%d / %d complete%n", "Progress:", complete, total));

            sb.append("\n  All jobs:\n");
            appendTree(sb, etlService.getChildExecutions(root), etlService, 0);
        }

        sb.append(LINE).append("\n");
        return sb.toString();
    }

    private void collectDescendants(JobExecution execution, EtlService etlService, List<JobExecution> result) {
        for (JobExecution child : etlService.getChildExecutions(execution)) {
            result.add(child);
            collectDescendants(child, etlService, result);
        }
    }

    private boolean isTerminal(JobExecutionStatus status) {
        return status == JobExecutionStatus.SUCCEEDED
                || status == JobExecutionStatus.FAILED
                || status == JobExecutionStatus.ABORTED;
    }

    private void appendTree(StringBuilder sb, List<JobExecution> executions, EtlService etlService, int depth) {
        String indent = buildIndent(depth);
        for (JobExecution exec : executions) {
            String status   = String.format("%-11s", exec.getStatus().toString());
            String duration = String.format("%10s", RunSummaryLogger.formatDuration(exec));
            sb.append(indent).append(status).append("  ").append(duration)
              .append("  ").append(RunSummaryLogger.label(exec)).append("\n");
            List<JobExecution> children = etlService.getChildExecutions(exec);
            if (!children.isEmpty()) {
                appendTree(sb, children, etlService, depth + 1);
            }
        }
    }

    private JobExecution findRoot(JobExecution execution, EtlService etlService) {
        JobExecution root = execution;
        while (root.getParentExecutionUuid() != null) {
            root = etlService.getJobExecution(root.getParentExecutionUuid());
        }
        return root;
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
