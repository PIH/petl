package org.openmrs.contrib.glimpse;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openmrs.contrib.glimpse.api.JobRunner;
import org.openmrs.contrib.glimpse.api.config.Config;
import org.openmrs.contrib.glimpse.api.config.DatabaseConnection;
import org.openmrs.contrib.glimpse.api.config.SourceEnvironment;
import org.openmrs.contrib.glimpse.api.config.TargetEnvironment;
import org.pentaho.di.core.logging.LogLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GlimpseApplicationTests {

    @Autowired
    GlimpseApplication app;

    @Test
    public void contextLoads() throws Exception {
        Assert.assertNotNull(app);
        Config config = app.getConfig();

        DatabaseConnection targetDbConnection = config.getTargetEnvironment().getDatabaseConnection();
        assertThat(targetDbConnection.getConnectionName(), is("Warehouse"));
        assertThat(targetDbConnection.getHostname(), is("localhost"));
        assertThat(targetDbConnection.getPort(), is(3308));
        assertThat(targetDbConnection.getDatabaseName(), is("pentaho_neno"));
        assertThat(targetDbConnection.getUsername(), is("root"));
        assertThat(targetDbConnection.getPassword(), is("root"));

        List<SourceEnvironment> sources = config.getSourceEnvironments();
        assertThat(sources.size(), is(1));

        SourceEnvironment source = sources.get(0);
        assertThat(source.getName(), is("Neno"));
        assertThat(source.getCountry(), is("malawi"));
        assertThat(source.getKeyPrefix(), is("100"));

        DatabaseConnection sourceDbConnection = source.getDatabaseConnection();
        assertThat(sourceDbConnection.getConnectionName(), is("OpenMRS"));
        assertThat(sourceDbConnection.getHostname(), is("localhost"));
        assertThat(sourceDbConnection.getPort(), is(3308));
        assertThat(sourceDbConnection.getDatabaseName(), is("openmrs"));
        assertThat(sourceDbConnection.getUsername(), is("root"));
        assertThat(sourceDbConnection.getPassword(), is("root"));
    }

    @Test
    @Ignore
    public void jobRunnerRunsJobs() throws Exception {

        TargetEnvironment targetEnvironment = app.getConfig().getTargetEnvironment();
        DatabaseConnection targetDb = targetEnvironment.getDatabaseConnection();
        List<SourceEnvironment> sources = app.getConfig().getSourceEnvironments();

        for (SourceEnvironment source : sources) {
            DatabaseConnection sourceDb = source.getDatabaseConnection();
            Map<String, String> parameters = new HashMap<>();
            parameters.put("pih.country", source.getCountry());
            parameters.put("openmrs.db.host", sourceDb.getHostname());
            parameters.put("openmrs.db.port", sourceDb.getPort().toString());
            parameters.put("openmrs.db.name", sourceDb.getDatabaseName());
            parameters.put("openmrs.db.user", sourceDb.getUsername());
            parameters.put("openmrs.db.password", sourceDb.getPassword());
            parameters.put("warehouse.db.host", targetDb.getHostname());
            parameters.put("warehouse.db.port", targetDb.getPort().toString());
            parameters.put("warehouse.db.name", targetDb.getDatabaseName());
            parameters.put("warehouse.db.user", targetDb.getUsername());
            parameters.put("warehouse.db.password", targetDb.getPassword());
            parameters.put("warehouse.db.key_prefix", source.getKeyPrefix());

            JobRunner jr = new JobRunner("/home/mseaton/code/pih-pentaho/malawi/jobs/refresh-warehouse.kjb");
            jr.setParameters(parameters);
            jr.setLogLevel(LogLevel.BASIC);
            jr.runJob();
        }
    }

}
