package org.pih.petl;

import org.junit.Assert;
import org.junit.Test;
import org.pih.petl.job.config.PetlConfig;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.HashMap;
import java.util.Map;

public class EnvironmentSubstitutionTest {

    private ApplicationConfig config(Map<String, Object> envVars, Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        MutablePropertySources sources = environment.getPropertySources();
        sources.replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, envVars));
        sources.addLast(new MapPropertySource("applicationYml", properties));
        return new ApplicationConfig(environment, new PetlConfig());
    }

    @Test
    public void resolvesDottedCamelCaseAndHyphenatedNamesFromSpringStyleEnvVars() {
        Map<String, Object> env = new HashMap<>();
        env.put("DATASOURCES_OPENMRS_CESCI_HOST", "openmrs-db");
        env.put("DATASOURCES_OPENMRS_CESCI_DATABASENAME", "openmrs");
        env.put("SPRING_DATASOURCE_DRIVER_CLASS_NAME", "org.h2.Driver");
        ApplicationConfig c = config(env, new HashMap<>());
        Assert.assertEquals("openmrs-db/openmrs/org.h2.Driver", c.replaceEnvironmentVariables(
                "${datasources.openmrs.cesci.host}/${datasources.openmrs.cesci.databaseName}/${spring.datasource.driver-class-name}"));
    }

    @Test
    public void applicationYmlValuesStillResolveAndEnvVarsOverrideThem() {
        Map<String, Object> yml = new HashMap<>();
        yml.put("datasources.warehouse.host", "from-yml");
        yml.put("datasources.warehouse.port", "1433");
        Map<String, Object> env = new HashMap<>();
        env.put("DATASOURCES_WAREHOUSE_HOST", "from-env");
        ApplicationConfig c = config(env, yml);
        Assert.assertEquals("from-env:1433", c.replaceEnvironmentVariables("${datasources.warehouse.host}:${datasources.warehouse.port}"));
    }

    @Test
    public void unresolvablePlaceholdersStayLiteralWithoutBreakingOthers() {
        Map<String, Object> env = new HashMap<>();
        env.put("DATASOURCES_OPENMRS_CESCI_HOST", "openmrs-db");
        ApplicationConfig c = config(env, new HashMap<>());
        Assert.assertEquals("openmrs-db ${datasources.openmrs.capitan.host}",
                c.replaceEnvironmentVariables("${datasources.openmrs.cesci.host} ${datasources.openmrs.capitan.host}"));
        Assert.assertNull(c.replaceEnvironmentVariables(null));
    }

    @Test
    public void jobParametersWinOverTheEnvironment() {
        Map<String, Object> env = new HashMap<>();
        env.put("SITENAME", "from-env");
        Map<String, String> params = new HashMap<>();
        params.put("siteName", "cesci");
        Assert.assertEquals("openmrs-cesci.yml", config(env, new HashMap<>()).getSubstitutedValue("openmrs-${siteName}.yml", params));
    }
}
