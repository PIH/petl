package org.pih.petl.job;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobExecutionRepository;
import org.pih.petl.job.schedule.PetlScheduledExecutionTask;
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
public class PetlScheduledExecutionTaskTest {

    @Autowired
    EtlService etlService;

    @Autowired
    PetlScheduledExecutionTask petlScheduledExecutionTask;

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
        System.clearProperty(ApplicationConfig.PETL_SCHEDULE_CRON);
        petlScheduledExecutionTask.execute(jobExecutionContext);
        assertThat(etlService.getLatestJobExecution("jobWithSchedule.yml").getStatus(), is("Execution Successful"));
        assertNull(etlService.getLatestJobExecution("jobWithoutSchedule.yml"));
    }

    @Test
    public void testRunningScheduledExecutionTaskShouldBothJobsIfGlobalScheduleProvided() throws Exception {
        jobExecutionRepository.deleteAll();
        System.setProperty(ApplicationConfig.PETL_SCHEDULE_CRON, "0 30 6 ? * *");
        petlScheduledExecutionTask.execute(jobExecutionContext);
        assertThat(etlService.getLatestJobExecution("jobWithSchedule.yml").getStatus(), is("Execution Successful"));
        assertThat(etlService.getLatestJobExecution("jobWithoutSchedule.yml").getStatus(), is("Execution Successful"));
    }

}
