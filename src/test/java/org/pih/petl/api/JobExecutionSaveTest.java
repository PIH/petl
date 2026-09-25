package org.pih.petl.api;

import org.junit.Assert;
import org.junit.Test;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.job.config.JobConfig;
import org.springframework.dao.CannotAcquireLockException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobExecutionSaveTest {

    @Test
    public void shouldRetrySaveOnLockFailure() {
        JobExecutionRepository repository = mock(JobExecutionRepository.class);
        JobExecution execution = new JobExecution();
        when(repository.save(any()))
                .thenThrow(new CannotAcquireLockException("deadlock"))
                .thenReturn(execution);
        EtlService etlService = new EtlService(mock(ApplicationConfig.class), repository);
        Assert.assertSame(execution, etlService.saveJobExecution(execution));
        verify(repository, times(2)).save(any());
    }

    @Test
    public void shouldFailSaveAfterMaxAttempts() {
        JobExecutionRepository repository = mock(JobExecutionRepository.class);
        when(repository.save(any())).thenThrow(new CannotAcquireLockException("deadlock"));
        EtlService etlService = new EtlService(mock(ApplicationConfig.class), repository);
        try {
            etlService.saveJobExecution(new JobExecution());
            Assert.fail("Expected CannotAcquireLockException");
        }
        catch (CannotAcquireLockException e) {
            verify(repository, times(5)).save(any());
        }
    }

    @Test
    public void shouldReturnFailedResultIfTaskCannotSaveExecution() {
        EtlService etlService = mock(EtlService.class);
        when(etlService.saveJobExecution(any())).thenThrow(new CannotAcquireLockException("deadlock"));
        JobExecutionResult result = new JobExecutionTask(etlService, new JobExecution(new JobConfig())).call();
        Assert.assertFalse(result.isSuccessful());
        Assert.assertTrue(result.getException() instanceof CannotAcquireLockException);
    }
}
