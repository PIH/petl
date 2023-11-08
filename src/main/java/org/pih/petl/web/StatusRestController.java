package org.pih.petl.web;

import org.pih.petl.api.EtlService;
import org.pih.petl.job.config.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@EnableAutoConfiguration
public class StatusRestController {

    @Autowired
    EtlService etlService;

    @GetMapping("/status/datasource/{datasource}")
    Map<String, Object> getDataSoureStatus(@PathVariable String datasource) {
        Map<String, Object> ret = new LinkedHashMap<>();
        ret.put("connected", false);
        ret.put("errorMessage", "");
        DataSource dataSource = etlService.getApplicationConfig().getEtlDataSource(datasource + ".yml");
        try (Connection connection = dataSource.openConnection()) {
            ret.put("databaseProductName", connection.getMetaData().getDatabaseProductName());
            ret.put("databaseProductVersion", connection.getMetaData().getDatabaseProductVersion());
            ret.put("connected", true);
        }
        catch (Exception e) {
            ret.put("errorMessage", e.getMessage());
        }
        return ret;
    }
}
