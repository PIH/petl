package org.pih.petl.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunSummaryLoggerTest {

    @Test
    public void shouldListSlowestLeafJobsInOrder() {
        JobExecution root = execution("root", null, 100);
        JobExecution site = execution("site", root, 90);
        JobExecution fast = execution("fast-table", site, 5);
        JobExecution slow = execution("slow-table", site, 80);
        JobExecution notStarted = execution("not-started", site, 0);
        notStarted.setStarted(null);

        Map<String, List<JobExecution>> children = new HashMap<>();
        children.put(root.getUuid(), Collections.singletonList(site));
        List<JobExecution> siteChildren = new ArrayList<>();
        siteChildren.add(fast);
        siteChildren.add(slow);
        siteChildren.add(notStarted);
        children.put(site.getUuid(), siteChildren);

        String summary = RunSummaryLogger.buildSummary(root, e -> children.getOrDefault(e.getUuid(), Collections.emptyList()));
        String slowest = summary.substring(summary.indexOf("Slowest jobs:"));
        Assert.assertTrue(slowest.indexOf("slow-table") < slowest.indexOf("fast-table"));
        Assert.assertFalse(slowest.contains("not-started"));
        Assert.assertFalse(slowest.contains("  site"));
    }

    private JobExecution execution(String description, JobExecution parent, int durationSeconds) {
        JobExecution execution = new JobExecution();
        execution.setDescription(description);
        if (parent != null) {
            execution.setParentExecutionUuid(parent.getUuid());
        }
        long now = System.currentTimeMillis();
        execution.setStarted(new Date(now - durationSeconds * 1000L));
        execution.setCompleted(new Date(now));
        execution.setStatus(JobExecutionStatus.SUCCEEDED);
        return execution;
    }
}
