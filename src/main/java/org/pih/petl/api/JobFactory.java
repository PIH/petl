package org.pih.petl.api;

import org.pih.petl.PetlException;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.CreateTableJob;
import org.pih.petl.job.IteratingJob;
import org.pih.petl.job.PentahoJob;
import org.pih.petl.job.RunMultipleJob;
import org.pih.petl.job.SqlJob;
import org.pih.petl.job.SqlServerImportJob;
import org.pih.petl.job.config.JobConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encapsulates a runnable pipeline
 */
public class JobFactory {

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
    public static boolean isValid(JobConfig config) {
        return getJobTypes().containsKey(config.getType());
    }

    /**
     * Instantiate a new ETL PetlJob from the given configuration file
     */
    public static PetlJob instantiate(JobConfig jobConfig) {
        Class<? extends PetlJob> type = getJobTypes().get(jobConfig.getType());
        if (type == null) {
            throw new PetlException("Invalid job type of " + jobConfig.getType());
        }
        try {
            return type.newInstance();
        }
        catch (Exception e) {
            throw new PetlException("Unable to instantiate job instance of type: " + type, e);
        }
    }
}
