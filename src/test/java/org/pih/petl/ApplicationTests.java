package org.pih.petl;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pih.petl.api.config.Config;
import org.pih.petl.api.config.StartupJobs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

    @Autowired
    Application app;

    @Test
    public void contextLoads() throws Exception {
        Assert.assertNotNull(app);
        Config config = app.getConfig();

        StartupJobs startupConfig = config.getStartupJobs();
        assertThat(startupConfig.isExitAutomatically(), is(true));
        List<String> startupJobs = startupConfig.getJobs();
        assertThat(startupJobs.size(), is(2));
        assertThat(startupJobs.get(0), is("/job1.properties"));
        assertThat(startupJobs.get(1), is("/job2.properties"));
    }
}
