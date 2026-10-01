package org.pih.petl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sets PETL's own datasource (its job history) from petl.jobStore: "h2" (default), a file under
 * petl.homeDir/data, or "sqlserver", the SQL Server in petl.sqlserver.* (PETL_SQLSERVER_* in the
 * environment). Added with the lowest precedence, so a spring.datasource set anywhere else (e.g.
 * an application.yml) wins.
 */
public class JobStoreEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    public static final String PROPERTY_SOURCE_NAME = "petlJobStore";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String store = environment.getProperty("petl.jobStore", "h2").trim().toLowerCase();
        Map<String, Object> p = new LinkedHashMap<>();
        if (store.equals("h2")) {
            p.put("spring.datasource.platform", "h2");
            p.put("spring.datasource.driver-class-name", "org.h2.Driver");
            p.put("spring.datasource.url", "jdbc:h2:file:${petl.homeDir}/data/petl;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE");
            p.put("spring.datasource.username", "sa");
            p.put("spring.datasource.password", "Test123");
            p.put("spring.liquibase.database-change-log-table", "PETL_DATABASE_CHANGE_LOG");
            p.put("spring.liquibase.database-change-log-lock-table", "PETL_DATABASE_CHANGE_LOG_LOCK");
        }
        else if (store.equals("sqlserver")) {
            for (String name : new String[] {"host", "database", "user", "password"}) {
                if (!environment.containsProperty("petl.sqlserver." + name)) {
                    throw new IllegalStateException("petl.jobStore is sqlserver, so petl.sqlserver." + name
                            + " (PETL_SQLSERVER_" + name.toUpperCase() + ") must be set");
                }
            }
            p.put("spring.datasource.platform", "mssql");
            p.put("spring.datasource.driver-class-name", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
            p.put("spring.datasource.url", "jdbc:sqlserver://${petl.sqlserver.host}:${petl.sqlserver.port:1433};databaseName=${petl.sqlserver.database}");
            p.put("spring.datasource.username", "${petl.sqlserver.user}");
            p.put("spring.datasource.password", "${petl.sqlserver.password}");
            p.put("spring.jpa.hibernate.dialect", "org.hibernate.dialect.SQLServer2012Dialect");
            p.put("spring.liquibase.database-change-log-table", "petl_database_change_log");
            p.put("spring.liquibase.database-change-log-lock-table", "petl_database_change_log_lock");
        }
        else {
            throw new IllegalStateException("petl.jobStore (PETL_JOBSTORE) must be h2 or sqlserver, not '" + store + "'");
        }
        environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, p));
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
