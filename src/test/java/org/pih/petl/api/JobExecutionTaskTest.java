package org.pih.petl.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.pih.petl.LogUtils;
import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfig;
import org.slf4j.MDC;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobExecutionTaskTest {

    @After
    public void teardown() {
        MDC.clear();
    }

    @Test
    public void shouldSetJobContextWhileExecutingAndRestoreAfter() {
        AtomicReference<String> contextDuringExecution = new AtomicReference<>();
        PetlJob petlJob = jobExecution -> contextDuringExecution.set(MDC.get(LogUtils.JOB_CONTEXT_KEY));
        JobExecution execution = execution("Importing from site to table");

        LogUtils.setJobContext("parent job");
        JobExecutionResult result = new JobExecutionTask(etlService(petlJob), execution).call();

        Assert.assertTrue(result.isSuccessful());
        Assert.assertEquals("Importing from site to table", contextDuringExecution.get());
        Assert.assertEquals("parent job", MDC.get(LogUtils.JOB_CONTEXT_KEY));
    }

    @Test
    public void shouldRestoreJobContextOnFailure() {
        PetlJob petlJob = jobExecution -> { throw new PetlException("Simulated"); };
        JobExecutionResult result = new JobExecutionTask(etlService(petlJob), execution("failing job")).call();
        Assert.assertFalse(result.isSuccessful());
        Assert.assertNull(MDC.get(LogUtils.JOB_CONTEXT_KEY));
    }

    private EtlService etlService(PetlJob petlJob) {
        EtlService etlService = mock(EtlService.class);
        when(etlService.getPetlJob(any())).thenReturn(petlJob);
        return etlService;
    }

    private JobExecution execution(String description) {
        JobConfig config = new JobConfig();
        config.setDescription(description);
        JobExecution execution = new JobExecution(config);
        return execution;
    }
}
