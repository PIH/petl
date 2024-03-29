package org.pih.petl.job;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecutionRepository;
import org.pih.petl.api.JobExecutionStatus;
import org.pih.petl.api.ScheduledExecutionTask;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"petl.jobDir = src/test/resources/configuration/jobs/scheduler"})
public class ScheduledExecutionTaskTest {

    @Autowired
    EtlService etlService;

    @Autowired
    ScheduledExecutionTask scheduledExecutionTask;

    @Autowired
    JobExecutionRepository jobExecutionRepository;

    JobExecutionContext jobExecutionContext;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Before
    public void setup() {
        jobExecutionContext = mock(JobExecutionContext.class);
        when(jobExecutionContext.getFireTime()).thenReturn(new Date());
    }

    @Test
    public void testRunningScheduledExecutionTaskShouldOnlyRunJobWithSchedule() throws Exception {
        jobExecutionRepository.deleteAll();
        scheduledExecutionTask.execute(jobExecutionContext);
        assertThat(etlService.getLatestJobExecution("jobWithSchedule.yml").getStatus(), is(JobExecutionStatus.SUCCEEDED));
        assertNull(etlService.getLatestJobExecution("jobWithoutSchedule.yml"));
    }
}
