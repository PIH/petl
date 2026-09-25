package org.pih.petl.api;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Prints a formatted run summary to stdout at the end of a top-level job execution.
 */
public class RunSummaryLogger {

    private static final String LINE     = "================================================================================";
    private static final String SUB_LINE = "  --------------------------------------------------------------------------------";
    private static final String ANSI_RED    = "[31m";
    private static final String ANSI_GREEN  = "[32m";
    private static final String ANSI_YELLOW = "[33m";
    private static final String ANSI_RESET  = "[0m";

    private static final int NUM_SLOWEST_JOBS = 10;

    public static void print(JobExecution execution, EtlService etlService) {
        // Cache child lookups, so that each execution's children are only queried once
        Map<String, List<JobExecution>> childCache = new HashMap<>();
        Function<JobExecution, List<JobExecution>> children = e -> childCache.computeIfAbsent(e.getUuid(), k -> etlService.getChildExecutions(e));
        System.out.println(buildSummary(execution, children));
    }

    static String buildSummary(JobExecution execution, Function<JobExecution, List<JobExecution>> childLookup) {
        StringBuilder sb = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        sb.append("\n").append(LINE).append("\n");
        sb.append("PETL Run Summary\n");
        sb.append(LINE).append("\n");
        sb.append(String.format("  %-12s%s%n",  "Job:",       label(execution)));
        sb.append(String.format("  %-12s%s%n",  "Status:",    colorStatus(execution.getStatus())));
        if (execution.getStarted() != null) {
            sb.append(String.format("  %-12s%s%n", "Started:",   sdf.format(execution.getStarted())));
        }
        if (execution.getCompleted() != null) {
            sb.append(String.format("  %-12s%s%n", "Completed:", sdf.format(execution.getCompleted())));
        }
        sb.append(String.format("  %-12s%s%n", "Duration:", formatDuration(execution)));

        List<JobExecution> children = childLookup.apply(execution);
        if (!children.isEmpty()) {
            sb.append("\n");
            appendTree(sb, children, childLookup, 0);
        }

        List<JobExecution> slowest = new ArrayList<>();
        collectLeaves(execution, childLookup, slowest);
        slowest.removeIf(e -> e.getStarted() == null || e == execution);
        slowest.sort(Comparator.comparingInt(JobExecution::getDurationSeconds).reversed());
        if (!slowest.isEmpty()) {
            sb.append("\nSlowest jobs:\n");
            for (JobExecution e : slowest.subList(0, Math.min(NUM_SLOWEST_JOBS, slowest.size()))) {
                sb.append(String.format("  %12s  %s%n", formatDuration(e), label(e)));
            }
        }

        List<JobExecution> failures = new ArrayList<>();
        collectLeafFailures(execution, childLookup, failures);
        if (!failures.isEmpty()) {
            sb.append("\nErrors:\n");
            for (JobExecution failed : failures) {
                sb.append(SUB_LINE).append("\n");
                sb.append("  ").append(ANSI_RED).append(label(failed)).append(ANSI_RESET).append("\n");
                if (failed.getErrorMessage() != null) {
                    sb.append("    ").append(failed.getErrorMessage()).append("\n");
                }
            }
            sb.append(SUB_LINE).append("\n");
        }

        sb.append(LINE);
        return sb.toString();
    }

    private static void collectLeaves(JobExecution execution, Function<JobExecution, List<JobExecution>> childLookup, List<JobExecution> leaves) {
        List<JobExecution> children = childLookup.apply(execution);
        if (children.isEmpty()) {
            leaves.add(execution);
        }
        for (JobExecution child : children) {
            collectLeaves(child, childLookup, leaves);
        }
    }

    private static void appendTree(StringBuilder sb, List<JobExecution> executions, Function<JobExecution, List<JobExecution>> childLookup, int depth) {
        String indent = indent(depth);
        for (JobExecution exec : executions) {
            String rawStatus = String.format("%-9s", exec.getStatus().toString());
            String duration  = String.format("%10s", formatDuration(exec));
            sb.append(indent)
              .append(colorStatus(exec.getStatus(), rawStatus))
              .append("  ").append(duration)
              .append("  ").append(label(exec))
              .append("\n");
            List<JobExecution> children = childLookup.apply(exec);
            if (!children.isEmpty()) {
                appendTree(sb, children, childLookup, depth + 1);
            }
        }
    }

    // Only collect failures that have no failed children — i.e. the actual root cause.
    // A parent that fails solely because a child failed is excluded.
    private static void collectLeafFailures(JobExecution execution, Function<JobExecution, List<JobExecution>> childLookup, List<JobExecution> failures) {
        List<JobExecution> children = childLookup.apply(execution);
        if (execution.getStatus() == JobExecutionStatus.FAILED) {
            boolean hasFailedChild = false;
            for (JobExecution child : children) {
                if (child.getStatus() == JobExecutionStatus.FAILED) {
                    hasFailedChild = true;
                    break;
                }
            }
            if (!hasFailedChild) {
                failures.add(execution);
                return;
            }
        }
        for (JobExecution child : children) {
            collectLeafFailures(child, childLookup, failures);
        }
    }

    static String formatDuration(JobExecution exec) {
        if (exec.getStarted() == null) { return "-"; }
        int total = exec.getDurationSeconds();
        int h = total / 3600;
        int m = (total % 3600) / 60;
        int s = total % 60;
        if (h > 0) { return String.format("%dh %02dm %02ds", h, m, s); }
        if (m > 0) { return String.format("%dm %02ds", m, s); }
        return String.format("%ds", s);
    }

    private static String colorStatus(JobExecutionStatus status, String text) {
        switch (status) {
            case SUCCEEDED: return ANSI_GREEN  + text + ANSI_RESET;
            case FAILED:    return ANSI_RED    + text + ANSI_RESET;
            case ABORTED:   return ANSI_YELLOW + text + ANSI_RESET;
            default:        return text;
        }
    }

    private static String colorStatus(JobExecutionStatus status) {
        return colorStatus(status, status.toString());
    }

    static String label(JobExecution exec) {
        if (exec.getJobPath() != null)     { return exec.getJobPath(); }
        if (exec.getDescription() != null) { return exec.getDescription(); }
        return "(unknown)";
    }

    private static String indent(int depth) {
        StringBuilder sb = new StringBuilder("  "); // base indent
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }
}
