package org.openmrs.contrib.glimpse;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GlimpseApplicationTests {

    @Autowired
    GlimpseApplication app;

    @Test
    public void contextLoads() {

        System.out.println(app.getDataSources());
        Assert.assertEquals(2, app.getDataSources().size());

    }

}
