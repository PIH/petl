package org.pih.petl;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringRunnerTest {

    @Autowired
    ApplicationConfig applicationConfig;

    static {
        setupEnvironment();
    }

    /**
     * Sets up the PETL Home Directory for a given unit test
     */
    public static File setupEnvironment() {
        File targetDir = new File("target");
        File petlHome = new File(targetDir, UUID.randomUUID().toString());
        File petlDataDir = new File(petlHome, "data");
        petlDataDir.mkdirs();
        System.setProperty("petl.homeDir", petlHome.getAbsolutePath());
        System.setProperty("petl.jobDir", "src/test/resources/configuration/jobs");
        System.setProperty("petl.datasourceDir", "src/test/resources/configuration/datasources");
        return petlHome;
    }

    @Test
    public void contextLoads() {
        Assert.assertNotNull(applicationConfig);
        Assert.assertNotNull(applicationConfig.getJobDir());
        Assert.assertNotNull(applicationConfig.getDataSourceDir());
    }
}
