package org.pih.petl.job.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.type.CreateTableJob;
import org.pih.petl.job.type.IteratingJob;
import org.pih.petl.job.type.PentahoJob;
import org.pih.petl.job.type.RunMultipleJob;
import org.pih.petl.job.type.SqlJob;
import org.pih.petl.job.type.SqlServerImportJob;

/**
 * Encapsulates a runnable pipeline
 */
public class PetlJobFactory {

    private static Map<String, Class<? extends PetlJob>> jobTypes = new LinkedHashMap<>();
    static {
        jobTypes.put("job-pipeline", RunMultipleJob.class);
        jobTypes.put("sqlserver-bulk-import", SqlServerImportJob.class);
        jobTypes.put("pentaho-job", PentahoJob.class);
        jobTypes.put("iterating-job", IteratingJob.class);
        jobTypes.put("sql-execution", SqlJob.class);
        jobTypes.put("create-table", CreateTableJob.class);
    }

    /**
     * @return the available job types in the system
     */
    public static Map<String, Class<? extends PetlJob>> getJobTypes() {
        return jobTypes;
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
