package org.pih.petl.job.schedule;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.SpringRunnerTest;
import org.pih.petl.api.EtlService;
import org.pih.petl.api.JobScheduler;
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

    static {
        SpringRunnerTest.setupEnvironment();
    }

    @Test
    public void testSimpleJobThatOutputsLoggingMessage() throws Exception {
        SimpleScheduleBuilder schedule = simpleSchedule().withIntervalInMilliseconds(1).withRepeatCount(4); // Run 5 times
        scheduler.schedule(SchedulerTestTask.class, schedule, 0);
        Thread.sleep(2000);
        Assert.assertEquals(5, SchedulerTestTask.numExecutions);
    }
}
