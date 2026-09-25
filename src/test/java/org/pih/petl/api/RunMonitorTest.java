package org.pih.petl.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.pih.petl.ApplicationConfig;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RunMonitorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    RunMonitor runMonitor;
    EtlService etlService;

    @Before
    public void setup() {
        ApplicationConfig applicationConfig = mock(ApplicationConfig.class);
        when(applicationConfig.getPetlHomeDir()).thenReturn(tempFolder.getRoot());
        runMonitor = new RunMonitor();
        ReflectionTestUtils.setField(runMonitor, "applicationConfig", applicationConfig);
        etlService = mock(EtlService.class);
    }

    @After
    public void teardown() {
        runMonitor.shutdown();
    }

    @Test
    public void shouldNotThrowIfDatabaseAccessFails() {
        when(etlService.getChildExecutions(any())).thenThrow(new CannotAcquireLockException("deadlock"));
        JobExecution root = execution("root", null, null);
        runMonitor.onSave(root);
        runMonitor.onJobStart(root, etlService);
        root.setStatus(JobExecutionStatus.FAILED);
        runMonitor.onJobComplete(root, etlService);
    }

    @Test
    public void shouldBuildStatusFromSavesWithoutQueryingEachEvent() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution root = execution("root", null, null);
        root.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(root);
        runMonitor.onJobStart(root, etlService);

        JobExecution child2 = execution("child-2", root, 2);
        JobExecution child1 = execution("child-1", root, 1);
        for (JobExecution child : new JobExecution[] {child2, child1}) {
            runMonitor.onSave(child);
            child.setStatus(JobExecutionStatus.IN_PROGRESS);
            runMonitor.onSave(child);
            runMonitor.onJobStart(child, etlService);
        }
        child1.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(child1);
        runMonitor.onJobComplete(child1, etlService);

        runMonitor.flushStatusFile(root.getUuid(), true);
        String status = readStatusFile();
        Assert.assertTrue(status.contains("1 / 2 complete"));
        Assert.assertTrue(status.indexOf("child-1") < status.indexOf("child-2"));

        // Existing children are only loaded from the database once, when the run is first seen
        verify(etlService, times(1)).getChildExecutions(any());
    }

    @Test
    public void shouldWriteFinalStatusWhenRootCompletes() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());
        JobExecution root = execution("root", null, null);
        runMonitor.onSave(root);
        runMonitor.onJobStart(root, etlService);
        root.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(root);
        runMonitor.onJobComplete(root, etlService);
        Assert.assertTrue(readStatusFile().contains("SUCCEEDED"));
    }

    @Test
    public void shouldHandleOverlappingRuns() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution runA = execution("run-a", null, null);
        JobExecution runB = execution("run-b", null, null);
        runA.setStatus(JobExecutionStatus.IN_PROGRESS);
        runB.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(runA);
        runMonitor.onJobStart(runA, etlService);
        runMonitor.onSave(runB);
        runMonitor.onJobStart(runB, etlService);

        // Run A completes while run B is the active run, and its final status is still written
        runA.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(runA);
        runMonitor.onJobComplete(runA, etlService);
        Assert.assertTrue(readStatusFile().contains("run-a"));
        Assert.assertTrue(readStatusFile().contains("SUCCEEDED"));

        // A final flush for a run that has already switched away is written for that run, not the active one
        runMonitor.flushStatusFile(runB.getUuid(), true);
        Assert.assertTrue(readStatusFile().contains("run-b"));

        // Run A has been released, and run B remains active and continues to be reported
        Map<String, JobExecution> executions = trackedExecutions();
        Assert.assertFalse(executions.containsKey(runA.getUuid()));
        Assert.assertEquals(runB.getUuid(), ReflectionTestUtils.getField(runMonitor, "activeRootUuid"));

        JobExecution childB = execution("child-b", runB, 1);
        runMonitor.onSave(childB);
        runMonitor.flushStatusFile(runB.getUuid(), false);
        Assert.assertTrue(readStatusFile().contains("child-b"));
    }

    @Test
    public void shouldSwitchActiveRunWhenActiveRunIsReleased() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution runA = execution("run-a", null, null);
        JobExecution runB = execution("run-b", null, null);
        runA.setStatus(JobExecutionStatus.IN_PROGRESS);
        runB.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(runA);
        runMonitor.onJobStart(runA, etlService);
        runMonitor.onSave(runB);
        runMonitor.onJobStart(runB, etlService);

        // Run B completes while run A is still in progress, so run A becomes the active run
        runB.setStatus(JobExecutionStatus.FAILED);
        runMonitor.onSave(runB);
        runMonitor.onJobComplete(runB, etlService);
        Assert.assertEquals(runA.getUuid(), ReflectionTestUtils.getField(runMonitor, "activeRootUuid"));

        runMonitor.flushStatusFile(runA.getUuid(), false);
        Assert.assertTrue(readStatusFile().contains("run-a"));
    }

    @Test
    public void shouldReleaseCompletedRunsThatAreNotActive() {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution runA = execution("run-a", null, null);
        JobExecution runB = execution("run-b", null, null);
        runA.setStatus(JobExecutionStatus.IN_PROGRESS);
        runB.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(runA);
        runMonitor.onJobStart(runA, etlService);
        runMonitor.onSave(runB);
        runMonitor.onJobStart(runB, etlService);

        // Run A completes without a completion notification, eg. a resumed run
        runA.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(runA);
        runMonitor.flushStatusFile(runB.getUuid(), false);
        Assert.assertFalse(trackedExecutions().containsKey(runA.getUuid()));
        Assert.assertTrue(trackedExecutions().containsKey(runB.getUuid()));
    }

    @Test
    public void shouldNotReloadRunOnRepeatCompletionNotification() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution root = execution("root", null, null);
        root.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(root);
        runMonitor.onJobStart(root, etlService);
        JobExecution child = execution("child-1", root, 1);
        child.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(child);

        // JobExecutor notifies completion of a top-level job twice: from executeInSeries and from executeJob
        root.setStatus(JobExecutionStatus.SUCCEEDED);
        runMonitor.onSave(root);
        runMonitor.onJobComplete(root, etlService);
        runMonitor.onSave(root);
        runMonitor.onJobComplete(root, etlService);

        verify(etlService, times(1)).getChildExecutions(any());
        Assert.assertTrue(trackedExecutions().isEmpty());
        Assert.assertTrue(readStatusFile().contains("child-1"));
    }

    @Test
    public void shouldTrackCompletedRunAgainWhenResumed() throws Exception {
        when(etlService.getChildExecutions(any())).thenReturn(Collections.emptyList());

        JobExecution root = execution("root", null, null);
        root.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(root);
        runMonitor.onJobStart(root, etlService);
        root.setStatus(JobExecutionStatus.FAILED);
        runMonitor.onSave(root);
        runMonitor.onJobComplete(root, etlService);
        Assert.assertTrue(trackedExecutions().isEmpty());

        // Resuming the failed run saves it as in progress, and its children are then reported again
        root.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(root);
        JobExecution child = execution("child-resumed", root, 1);
        child.setStatus(JobExecutionStatus.IN_PROGRESS);
        runMonitor.onSave(child);
        runMonitor.onJobStart(child, etlService);
        runMonitor.flushStatusFile(root.getUuid(), true);
        Assert.assertTrue(readStatusFile().contains("child-resumed"));
        Assert.assertEquals(root.getUuid(), ReflectionTestUtils.getField(runMonitor, "activeRootUuid"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, JobExecution> trackedExecutions() {
        return (Map<String, JobExecution>) ReflectionTestUtils.getField(runMonitor, "executions");
    }

    private JobExecution execution(String description, JobExecution parent, Integer sequenceNum) {
        JobExecution execution = new JobExecution();
        execution.setDescription(description);
        if (parent != null) {
            execution.setParentExecutionUuid(parent.getUuid());
        }
        execution.setSequenceNum(sequenceNum);
        return execution;
    }

    private String readStatusFile() throws Exception {
        File f = new File(new File(tempFolder.getRoot(), "logs"), "petl-status.log");
        return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
    }
}
