package org.pih.petl;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.pih.petl.api.config.ConfigLoader;
import org.pih.petl.api.config.EtlJobConfig;
import org.pih.petl.api.job.LoadSqlServerJob;
import org.pih.petl.api.status.EtlStatusTable;

/**
 *
 */
public class LoadSqlServerJobTest {

    @Before
    public void setConfigurationDirectory() {
        String path = "src/test/resources/configuration";
        File configDir = new File(path);
        ConfigLoader.setConfigDirectory(configDir);
        EtlStatusTable.createStatusTable();
    }

    @Test
    public void testLoadingEncounterTypes() throws Exception {
        EtlJobConfig etlJobConfig = ConfigLoader.getEtlJobConfigFromFile("jobs/encountertypetest/job.yml");
        LoadSqlServerJob job = new LoadSqlServerJob(etlJobConfig);
        job.execute();
    }

    @Test
    public void testLoadingVaccinationsAnc() throws Exception {
        EtlJobConfig etlJobConfig = ConfigLoader.getEtlJobConfigFromFile("jobs/vaccinations_anc/job.yml");
        LoadSqlServerJob job = new LoadSqlServerJob(etlJobConfig);
        job.execute();
    }
}
