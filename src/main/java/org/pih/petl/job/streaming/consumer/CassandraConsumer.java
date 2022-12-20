package org.pih.petl.job.streaming.consumer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.petl.PetlException;
import org.pih.petl.job.streaming.DebeziumEvent;
import org.pih.petl.job.streaming.DebeziumOperation;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * See:  https://github.com/datastax/java-driver/tree/4.x/manual/core
 */
public class CassandraConsumer extends DebeziumConsumer {

    private final Log log = LogFactory.getLog(getClass());

    CqlSession session;
    private static final JsonMapper mapper = new JsonMapper();

    @Override
    public void startup() {
        super.startup();

        String host = "localhost";
        int port = 9042;
        String datacenter = "datacenter1";
        String keyspaceName = "openmrs";
        String replicationStrategy = "SimpleStrategy";
        int replicationFactor = 1;

        try (CqlSession keyspaceSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(datacenter)
                .build()) {
            keyspaceSession.execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName +
                            " WITH replication = {" +
                            "'class':'" + replicationStrategy + "','replication_factor':" + replicationFactor +
                            "};"
            );
        }

        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspaceName)
                .build();

        // Create encounters table
        session.execute("drop table if exists encounters;");
        executeQueryAtResource("/streaming/encounters.sql");
    }

    protected void executeQueryAtResource(String resourceName) {
        try {
            String data = IOUtils.toString(getClass().getResourceAsStream(resourceName), "UTF-8");
            session.execute(data);
        }
        catch (Exception e) {
            throw new PetlException(e);
        }
    }

    /**
     * TODO Handle deletes and voided rows
     */
    @Override
    public void accept(DebeziumEvent event) {
        String table = event.getTable();
        String uuid = event.getUuid();
        Map<String, Object> data = event.getValues();

        if (table.equalsIgnoreCase("encounter")) {
            data.put("id", data.remove("uuid"));
            data.put("server_id", event.getServerName());
            if (event.getOperation() != DebeziumOperation.DELETE) {
                try {
                    session.execute("INSERT INTO encounters JSON '" + mapper.writeValueAsString(data) + "';");
                }
                catch (Exception e) {
                    throw new PetlException(e);
                }
            }
            else {
                session.execute("DELETE FROM encounters where id = ?", uuid);
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (session != null) {
            try {
                session.close();
            }
            catch (Exception e) {
                log.warn("An error occurred while closing the CqlSession", e);
            }
        }
    }
}
