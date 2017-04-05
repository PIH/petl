package org.openmrs.contrib.glimpse;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openmrs.contrib.glimpse.api.JobRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GlimpseApplicationTests {

    @Autowired
    GlimpseApplication app;

    @Test
    public void contextLoads() throws Exception {
        Assert.assertNotNull(app);
        {
            MysqlDataSource omrsDb = app.openmrsDataSource();
            Assert.assertTrue(omrsDb.getURL() != null && omrsDb.getUrl().contains("localhost:3306"));
            Assert.assertEquals("root", omrsDb.getUser());
        }
        {
            MysqlDataSource analysisDb = app.analysisDataSource();
            Assert.assertTrue(analysisDb.getURL() != null && analysisDb.getUrl().contains("localhost:3306"));
            Assert.assertEquals("root", analysisDb.getUser());
        }
    }

    @Test
    public void jobRunnerRunsJobs() throws Exception {
        JobRunner jr = new JobRunner("/home/mseaton/code/pih-pentaho/malawi/jobs/refresh-warehouse.kjb");
        jr.runJob();
    }

}
