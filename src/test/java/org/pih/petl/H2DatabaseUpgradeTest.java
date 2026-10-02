package org.pih.petl;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;

public class H2DatabaseUpgradeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldMoveAnH2Version1DatabaseAsideSoANewOneCanBeCreated() throws Exception {
        File dir = folder.newFolder("data");
        File databaseFile = new File(dir, "petl.mv.db");
        copyVersion1Database(databaseFile);

        upgrade("jdbc:h2:file:" + dir + "/petl;DB_CLOSE_ON_EXIT=FALSE");

        File moved = new File(dir, "petl.mv.db" + H2DatabaseUpgrade.MOVED_SUFFIX);
        Assert.assertTrue(moved.isFile());
        Assert.assertTrue(H2DatabaseUpgrade.isH2Version1File(moved));
        Assert.assertFalse(databaseFile.exists());
        try (Connection c = DriverManager.getConnection("jdbc:h2:file:" + dir + "/petl", "sa", "Test123");
             ResultSet rs = c.createStatement().executeQuery("select 1")) {
            Assert.assertTrue(rs.next());
        }
        Assert.assertFalse(H2DatabaseUpgrade.isH2Version1File(databaseFile));
    }

    @Test
    public void shouldLeaveAnH2Version2DatabaseAlone() throws Exception {
        File dir = folder.newFolder("data");
        try (Connection c = DriverManager.getConnection("jdbc:h2:file:" + dir + "/petl", "sa", "Test123")) {
            c.createStatement().execute("create table t (i int)");
        }
        upgrade("jdbc:h2:file:" + dir + "/petl");
        Assert.assertTrue(new File(dir, "petl.mv.db").isFile());
        Assert.assertFalse(new File(dir, "petl.mv.db" + H2DatabaseUpgrade.MOVED_SUFFIX).exists());
    }

    @Test
    public void shouldRefuseToOverwriteAnEarlierMovedDatabase() throws Exception {
        File dir = folder.newFolder("data");
        copyVersion1Database(new File(dir, "petl.mv.db"));
        copyVersion1Database(new File(dir, "petl.mv.db" + H2DatabaseUpgrade.MOVED_SUFFIX));
        try {
            upgrade("jdbc:h2:file:" + dir + "/petl");
            Assert.fail("Expected PetlException");
        }
        catch (PetlException e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
        }
    }

    @Test
    public void shouldIgnoreUrlsThatAreNotH2Files() {
        Assert.assertNull(H2DatabaseUpgrade.getDatabaseFile("jdbc:sqlserver://localhost:1433;databaseName=openmrs_reporting"));
        Assert.assertNull(H2DatabaseUpgrade.getDatabaseFile("jdbc:h2:mem:petl"));
        Assert.assertNull(H2DatabaseUpgrade.getDatabaseFile(null));
        Assert.assertEquals(new File("/home/petl/data/petl.mv.db"),
                H2DatabaseUpgrade.getDatabaseFile("jdbc:h2:file:/home/petl/data/petl;DB_CLOSE_ON_EXIT=FALSE"));
        Assert.assertFalse(H2DatabaseUpgrade.isH2Version1File(new File(folder.getRoot(), "missing.mv.db")));
    }

    private void upgrade(String url) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test",
                Collections.singletonMap("spring.datasource.url", url)));
        new H2DatabaseUpgrade().onApplicationEvent(
                new ApplicationEnvironmentPreparedEvent(new SpringApplication(), new String[0], environment));
    }

    // A database written by H2 1.4.199, as earlier versions of PETL left in ${petl.homeDir}/data
    private void copyVersion1Database(File target) throws Exception {
        FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/h2/petl-h2-1.4.199.mv.db"), target);
    }
}
