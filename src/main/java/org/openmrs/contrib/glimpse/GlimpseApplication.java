package org.openmrs.contrib.glimpse;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

/**
 * This mainly just serves to demonstrate that we can start up and run our Spring Boot application.
 * Run this class from Intellij, and see it start up without errors
 *
 * The Configuration, EnableConfigurationProperties, and ConfigurationProperties annotations work to
 * enable the use of an application.properties or application.yml file to pass in configuration to the application.
 * There is good documentation on how we can set this up to have overrides in various environments here:
 * http://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html
 *
 * These can go on any class (eg. we could have a new Configuration bean, but the application bean is often used by convention
 *
 * The below properties are all able to be set via these configuration files
 */
@SpringBootApplication
@EnableConfigurationProperties
@Configuration
public class GlimpseApplication {

	public static void main(String[] args) {
		SpringApplication.run(GlimpseApplication.class, args);
	}

    @ConfigurationProperties(prefix = "datasource.openmrs")
    @Bean
    MysqlDataSource openmrsDataSource() throws SQLException {
        return new MysqlDataSource();  // TODO: Add ability to specify a different database type
    }

    @ConfigurationProperties(prefix = "datasource.analysis")
    @Bean
    MysqlDataSource analysisDataSource() throws SQLException {
        return new MysqlDataSource();  // TODO: Add ability to specify a different database type
    }
}
