package org.pih.petl.job.type;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.ApplicationConfig;
import org.pih.petl.PetlException;
import org.pih.petl.api.ExecutionContext;
import org.pih.petl.job.PetlJob;
import org.pih.petl.job.config.JobConfiguration;
import org.pih.petl.job.datasource.DatabaseUtil;
import org.pih.petl.job.datasource.EtlDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PetlJob that can create a table
 */
public class CreateTableJob implements PetlJob {

    private static final Log log = LogFactory.getLog(CreateTableJob.class);

    public static final List<String> TYPES_WITH_SIZES = Arrays.asList(
      "VARCHAR", "CHAR", "DECIMAL", "DOUBLE"
    );

    /**
     * Creates a new instance of the job
     */
    public CreateTableJob() {
    }

    /**
     * @see PetlJob
     */
    @Override
    public void execute(final ExecutionContext context) throws Exception {

        ApplicationConfig appConfig = context.getApplicationConfig();
        JobConfiguration config = context.getJobConfig().getConfiguration();

        context.setStatus("Loading configuration");

        // Get source datasource
        String sourceDataSourceFilename = config.getString("createFromTable", "datasource");
        EtlDataSource sourceDatasource = appConfig.getEtlDataSource(sourceDataSourceFilename);
        String sourceTable = config.getString("createFromTable", "tableName");

        // Get target datasource
        String targetDataFileName = config.getString("target", "datasource");
        EtlDataSource targetDatasource = appConfig.getEtlDataSource(targetDataFileName);
        String targetTable = config.getString("target", "tableName");

        boolean dropIfExists = false;
        boolean skipIfExists = false;

        String actionIfExists = config.getString("target", "ifTableExists");
        if (StringUtils.isNotEmpty(actionIfExists)) {
            if (actionIfExists.equalsIgnoreCase("DROP")) {
                dropIfExists = true;
            }
            else if (actionIfExists.equalsIgnoreCase("SKIP")) {
                skipIfExists = true;
            }
            else {
                throw new IllegalArgumentException("Unknown value of " + actionIfExists + " for ifTableExists");
            }
        }

        // Check if table already exists

        boolean tableAlreadyExists = false;
        try (Connection targetConnection = DatabaseUtil.openConnection(targetDatasource)) {
            DatabaseMetaData metaData = targetConnection.getMetaData();
            ResultSet matchingTables = metaData.getTables(null, null, targetTable, new String[] {"TABLE"});
            if (matchingTables.next()) {
                if (dropIfExists) {
                    try (Statement statement = targetConnection.createStatement()) {
                        context.setStatus("Dropping existing table");
                        statement.execute("drop table " + targetTable);
                    }
                }
                else {
                    tableAlreadyExists = true;
                }
            }
        }

        if (tableAlreadyExists) {
            if (skipIfExists) {
                log.debug("Table " + targetTable + " already exists, and configured to skipIfExists, returning");
                return;
            }
            throw new PetlException("Table already exists and not configured to drop existing table");
        }

        String schema = null;

        // Load in table schema from the provided data source and table
        if (sourceDatasource != null && StringUtils.isNotEmpty(sourceTable)) {
            StringBuilder stmt = new StringBuilder();
            try (Connection sourceConnection = DatabaseUtil.openConnection(sourceDatasource)) {
                DatabaseMetaData metaData = sourceConnection.getMetaData();
                // First create a columnMap as there seem to be cases where the same column is returned multiple times
                Map<String, String> columnMap = new LinkedHashMap<>();
                ResultSet columns = metaData.getColumns(null, null, sourceTable, null);
                while (columns.next()) {
                    String type = columns.getString("TYPE_NAME");
                    if (TYPES_WITH_SIZES.contains(type)) {
                        String size = columns.getString("COLUMN_SIZE");
                        String digits = columns.getString("DECIMAL_DIGITS");
                        if (StringUtils.isNotEmpty(size)) {
                            type += " (" + size;
                            if (StringUtils.isNotEmpty(digits)) {
                                type += "," + digits;
                            }
                            type += ")";
                        }
                    }
                    columnMap.put(columns.getString("COLUMN_NAME"), type);
                }
                // Iterate over this columnMap to generate a create table statement
                for (String columnName : columnMap.keySet()) {
                    String columnType = columnMap.get(columnName);
                    if (stmt.length() == 0) {
                        stmt.append("create table ").append(targetTable).append(" (");
                    }
                    else {
                        stmt.append(", ");
                    }
                    stmt.append(System.lineSeparator()).append(columnName).append(" ").append(columnType);
                }
            }
            stmt.append(")");
            schema = stmt.toString();
        }

        if (schema != null) {
            try (Connection targetConnection = DatabaseUtil.openConnection(targetDatasource)) {
                try (Statement statement = targetConnection.createStatement()) {
                    log.warn("Executing: " + schema);
                    statement.execute(schema);
                }
            }
        }
    }
}
