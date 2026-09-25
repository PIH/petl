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

        runMonitor.flushStatusFile(true);
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
