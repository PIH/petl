package org.pih.petl;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;

import java.util.Collections;

public class JobStoreEnvironmentPostProcessorTest {

    private MockEnvironment process(MockEnvironment environment) {
        new JobStoreEnvironmentPostProcessor().postProcessEnvironment(environment, null);
        return environment;
    }

    @Test
    public void defaultsToH2UnderTheHomeDirectory() {
        MockEnvironment e = process(new MockEnvironment().withProperty("petl.homeDir", "/home/petl"));
        Assert.assertEquals("org.h2.Driver", e.getProperty("spring.datasource.driver-class-name"));
        Assert.assertEquals("jdbc:h2:file:/home/petl/data/petl;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE", e.getProperty("spring.datasource.url"));
        Assert.assertEquals("PETL_DATABASE_CHANGE_LOG", e.getProperty("spring.liquibase.database-change-log-table"));
    }

    @Test
    public void sqlserverUsesThePetlSqlserverSettings() {
        MockEnvironment e = process(new MockEnvironment()
                .withProperty("petl.jobStore", "sqlserver")
                .withProperty("petl.sqlserver.host", "sqlserver")
                .withProperty("petl.sqlserver.database", "openmrs_ces_ci")
                .withProperty("petl.sqlserver.user", "petl")
                .withProperty("petl.sqlserver.password", "pw"));
        Assert.assertEquals("jdbc:sqlserver://sqlserver:1433;databaseName=openmrs_ces_ci", e.getProperty("spring.datasource.url"));
        Assert.assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", e.getProperty("spring.datasource.driver-class-name"));
        Assert.assertEquals("petl", e.getProperty("spring.datasource.username"));
        Assert.assertEquals("org.hibernate.dialect.SQLServer2012Dialect", e.getProperty("spring.jpa.hibernate.dialect"));
        Assert.assertEquals("petl_database_change_log", e.getProperty("spring.liquibase.database-change-log-table"));
        Assert.assertEquals("petl_database_change_log_lock", e.getProperty("spring.liquibase.database-change-log-lock-table"));
    }

    @Test
    public void anExplicitSpringDatasourceStillWins() {
        MockEnvironment e = new MockEnvironment().withProperty("petl.jobStore", "sqlserver")
                .withProperty("petl.sqlserver.host", "sqlserver").withProperty("petl.sqlserver.database", "db")
                .withProperty("petl.sqlserver.user", "u").withProperty("petl.sqlserver.password", "p");
        e.getPropertySources().addFirst(new MapPropertySource("externalApplicationYml",
                Collections.<String, Object>singletonMap("spring.datasource.url", "jdbc:sqlserver://legacy:1433;databaseName=x")));
        Assert.assertEquals("jdbc:sqlserver://legacy:1433;databaseName=x", process(e).getProperty("spring.datasource.url"));
    }

    @Test(expected = IllegalStateException.class)
    public void sqlserverWithoutItsSettingsFailsAtStartup() {
        process(new MockEnvironment().withProperty("petl.jobStore", "sqlserver"));
    }

    @Test(expected = IllegalStateException.class)
    public void anUnknownJobStoreFailsAtStartup() {
        process(new MockEnvironment().withProperty("petl.jobStore", "mysql"));
    }
}
