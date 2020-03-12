package org.pih.petl.job.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.type.PentahoJob;
import org.pih.petl.job.type.RunMultipleJob;
import org.pih.petl.job.type.SqlServerImportJob;

/**
 * Encapsulates a runnable pipeline
 */
public class PetlJobFactory {

    /**
     * @return the available job types in the system
     */
    public static Map<String, Class<? extends PetlJob>> getJobTypes() {
        Map<String, Class<? extends PetlJob>> m = new LinkedHashMap<>();
        m.put("job-pipeline", RunMultipleJob.class);
        m.put("sqlserver-bulk-import", SqlServerImportJob.class);
        m.put("pentaho-job", PentahoJob.class);
        return m;
    }

    /**
     * Returns true if the PetlJobConfig is valid
     * TODO: Expand on this
     */
    public static boolean isValid(PetlJobConfig config) {
        return getJobTypes().containsKey(config.getType());
    }

    /**
     * Instantiate a new ETL PetlJob from the given configuration file
     */
    public static PetlJob instantiate(PetlJobConfig jobConfig) {
        try {
            Class<? extends PetlJob> type = getJobTypes().get(jobConfig.getType());
            if (type == null) {
                throw new PetlException("Invalid job type of " + jobConfig.getType());
            }
            return type.newInstance();
        }
        catch (Exception e) {
            throw new PetlException("Unable to instantiate job using reflection, passing EtlService and job path", e);
        }
    }
}
