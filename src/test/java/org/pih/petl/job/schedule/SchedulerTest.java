package org.pih.petl.job.schedule;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobScheduler;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests the SqlServerImportJob
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SchedulerTest {

    @Autowired
    EtlService etlService;

    @Autowired
    JobScheduler scheduler;

    @Autowired
    Scheduler quartzScheduler;

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @After
    public void tearDown() throws Exception {
        quartzScheduler.deleteJob(JobKey.jobKey(SchedulerTestTask.class.getSimpleName()));
    }

    @Test
    public void testSimpleJobThatOutputsLoggingMessage() throws Exception {
        SchedulerTestTask.numExecutions = 0;
        // 10ms interval avoids Quartz's misfire threshold under CPU contention; smart-policy
        // with a finite repeat count was occasionally dropping fires when paired with a 1ms
        // interval and a busy-wait observer.
        SimpleScheduleBuilder schedule = simpleSchedule()
                .withIntervalInMilliseconds(10)
                .withRepeatCount(4) // Run 5 times
                .withMisfireHandlingInstructionNowWithExistingCount();
        scheduler.schedule(SchedulerTestTask.class, schedule, 0);
        long maxTimeToWait = System.currentTimeMillis() + 30_000;
        while (SchedulerTestTask.numExecutions < 5 && System.currentTimeMillis() < maxTimeToWait) {
            Thread.sleep(25);
        }
        Assert.assertEquals(5, SchedulerTestTask.numExecutions);
    }
}
